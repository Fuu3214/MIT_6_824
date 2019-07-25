package raft

import (
	"sync/atomic"
	"time"
)

// startElection
func (rf *Raft) leaderElection() {
	electionTimer := time.NewTimer(electionTimeOut())
	defer electionTimer.Stop()
	for {
		voteCh := make(chan struct{})
		voteComplete := make(chan struct{})
		resetTimer(electionTimer, electionTimeOut())

		rf.mu.Lock()
		if !rf.isStale {
			// important, leaderElection may block at mutex because in follower state
			// it recieves a RPC, if it become stale then leaderElection should abort
			// otherwise we have a candidate with false term!

			rf.convertToCandidate()

			lastIdx := rf.getLastLogIndex()
			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  rf.getLog(lastIdx).Term,
			}
			var countVote int32 = 1
			for i := 0; i < rf.numServer(); i++ {
				if i == rf.id { //don't send to yourself
					continue
				}
				go rf.election(args, i, voteCh, voteComplete, &countVote)
			}
		}
		rf.mu.Unlock()

		select {
		case <-rf.killCh: //terminate
			return
		case <-rf.staleSignal:
			// if it hear from a new leader from the same term, it should also go here
			// convert to follower mode
			DPrintfElection("%d recieves stale signal at term: %d, convert to follower", rf.id, rf.CurrentTerm)
			go rf.listen()
			return
		case <-voteCh:
			DPrintf("id %d elected, term: %d", rf.id, rf.CurrentTerm)
			voteCh = nil //gc go to work!
			close(voteComplete)
			rf.mu.Lock()
			if rf.isStale {
				go rf.listen()
				rf.mu.Unlock()
				return
			}
			rf.convertToLeader()
			rf.mu.Unlock()
			go rf.broadCast(true)
			// convert to leader mode
			return
		case <-electionTimer.C:
			//restart election
			voteCh = nil //gc go to work!
			close(voteComplete)
			electionTimer.Reset(KEEPALIVEINTERVAL) // remove it then dealock
		}
	}
}

func (rf *Raft) election(args *RequestVoteArgs, server int, voteCh chan struct{}, voteComplete chan struct{}, countVote *int32) {
	var reply RequestVoteReply
	DPrintfElection("%d sending requestVote to: %d, term: %d", rf.id, server, rf.CurrentTerm)
	if rf.callRequestVote(server, args, &reply) { //blocking call
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm { // RPC reply has larger term
			rf.VotedFor = NULL
			DPrintfElection("id: %d term smaller than rpc reply, term: %d", rf.id, rf.CurrentTerm)
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
		// RPC response may come after candidate starts a new election
		if rf.state != CANDIDATE || rf.CurrentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// only candidate can do the following operations!
		if reply.VoteGranted {
			atomic.AddInt32(countVote, 1)
			DPrintfElection("id：%d, countVote: %d, term: %d", rf.id, *countVote, rf.CurrentTerm)
		}
		if atomic.LoadInt32(countVote) > int32(len(rf.peers)/2) {
			//do something to convert to leader but prevent converting multiple times
			sendWithCancellation(voteCh, voteComplete)
		}
	}
}
