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
		if !rf.staleState {
			numServer := rf.numServer()
			for i := 0; i < numServer; i++ {
				if rf.id == i {
					continue
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
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
			DPrintf("%d recieves stale signal at term: %d, convert to follower", rf.id, rf.currentTerm)
			go rf.listen()
			return
		case <-heartBeatTimer.C:
			//send heartbeat
			heartBeatTimer.Reset(KEEPALIVEINTERVAL) // remove it and deadlock
		}
	}
}

func (rf *Raft) sync(args *AppendEntriesArgs, server int) {
	DPrintf("%d send append entries to: %d at term: %d", rf.id, server, rf.currentTerm)
	var reply AppendEntriesReply
	if rf.callAppendEntries(server, args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm { // RPC reply has larger term
			rf.votedFor = NULL
			DPrintf("id: %d term smaller than rpc reply, term: %d", rf.id, rf.currentTerm)
			rf.stale(reply.Term)
			rf.mu.Unlock()
			return
		}

		if rf.state != LEADER || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Succcess {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		} else { // retry
			rf.nextIndex[server]--
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.getLog(rf.nextIndex[server] - 1).Term
			args.Entries = rf.getLogs(rf.nextIndex[server], rf.getLastLogIndex()+1)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIdx() {
	rf.matchIndex[rf.id] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied <= rf.commitIndex {
		rf.lastApplied++
		rf.apply(rf.getLog(rf.lastApplied))
	}
}

func (rf *Raft) apply(*RaftLog) {
	//TO DO
}
