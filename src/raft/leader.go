package raft

import (
	"sort"
	"time"
)

func (rf *Raft) broadCast(empty bool) {
	heartBeatTimer := time.NewTimer(heartBeat())
	defer heartBeatTimer.Stop()
	for {
		resetTimer(heartBeatTimer, heartBeat())
		rf.mu.Lock()
		if !rf.isStale {
			rf.nextIndex[rf.id] = rf.getLastLogIndex() + 1 // nextidx for leader itsself should always be last idx + 1
			rf.matchIndex[rf.id] = rf.getLastLogIndex()    // matchidx for leader itself should always be last idx
			for i := 0; i < rf.numServer(); i++ {
				if rf.id == i {
					continue
				}
				args := &AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderID:     rf.id,
					LeaderCommit: rf.commitIndex,
				}
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.getLog(rf.nextIndex[i] - 1).Term

				if empty || rf.getLastLogIndex() < rf.nextIndex[i] { // first or idle
					args.Entries = make([]RaftLog, 0)
				} else {
					args.Entries = rf.getLogs(rf.nextIndex[i], rf.getLastLogIndex()+1)
				}
				go rf.sync(args, i)
			}
		}
		rf.mu.Unlock()

		select {
		case <-rf.killCh: //terminate
			return
		case <-rf.staleSignal:
			// leader stale, convert to follower mode
			DPrintf("%d recieves stale signal at term: %d, convert to follower", rf.id, rf.CurrentTerm)
			go rf.listen()
			return
		case <-heartBeatTimer.C:
			//send heartbeat
			heartBeatTimer.Reset(KEEPALIVEINTERVAL) // if remove it then deadlock
			empty = false                           // don't enforce it to be empty
		}
	}
}

func (rf *Raft) sync(args *AppendEntriesArgs, server int) {
	for {
		DPrintf("%d send append entries to: %d at term: %d, state: %d", rf.id, server, rf.CurrentTerm, rf.state)
		var reply AppendEntriesReply
		if rf.callAppendEntries(server, args, &reply) {
			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm { // RPC reply has larger term
				rf.VotedFor = NULL
				DPrintf("id: %d term smaller than rpc reply, term: %d", rf.id, rf.CurrentTerm)
				sendStale := false
				if rf.state != FOLLOWER {
					sendStale = true
				}
				rf.convertToFollower(reply.Term)
				rf.mu.Unlock()
				if sendStale {
					rf.stale()
				}
				return
			}

			if rf.state != LEADER || rf.CurrentTerm != args.Term || reply.Term < rf.CurrentTerm {
				// leader is stale or follower is stale, ignore
				rf.mu.Unlock()
				return
			}

			if reply.Succcess {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				DPrintfAgreement("reply from %d to %d says success, nextidx: %v, matchidx: %v", server, rf.id, rf.nextIndex, rf.matchIndex)
				rf.updateCommitIdx()
				rf.mu.Unlock()
				return
			} else { // retry
				if reply.ConflictIdx == NULL && reply.ConflictTerm == NULL {
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[server] = rf.searchForNextIdx(&reply)
				args.PrevLogIndex = rf.nextIndex[server] - 1
				DPrintfAgreement("reply from %d to %d says fail, nextidx: %v, matchidx: %v, args.PrevLogIndex: %d", server, rf.id, rf.nextIndex, rf.matchIndex, args.PrevLogIndex)
				args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
				args.Entries = rf.getLogs(rf.nextIndex[server], rf.getLastLogIndex()+1)
			}
			rf.mu.Unlock()
		} else {
			// madezhizhang 0v0 ???????????????????????????????????????????
			return // without which we keep sending 6000+ RPC, because we loop infinitely.......
		}
	}
}

func (rf *Raft) searchForNextIdx(reply *AppendEntriesReply) int {
	var res int
	if reply.ConflictIdx > rf.getLastLogIndex() {
		res = rf.getLastLogIndex() + 1
	}
	if rf.getLog(reply.ConflictIdx).Term == reply.ConflictTerm {
		// if agreed, nextidx should be reply.ConflictIdx + 1
		res = reply.ConflictIdx + 1
	} else {
		// if not agreed, search for it
		idx := reply.ConflictIdx
		for !rf.isLogBegining(idx) && rf.getLog(idx).Term > reply.ConflictTerm {
			// find a term smaller or equal to reply.conflictTerm
			idx--
		}
		tmpTerm := rf.getLog(idx).Term
		for !rf.isLogBegining(idx) && rf.getLog(idx).Term == tmpTerm {
			// find first index of this term
			idx--
		}
		res = idx + 1
	}
	return res
}

func (rf *Raft) updateCommitIdx() {
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.CurrentTerm {
		rf.commitIndex = N
		DPrintfAgreement("id: %d update commitIndex to %d", rf.id, N)
		rf.applyLogs(rf.commitIndex)
	}
}
