package raft

import (
	"sync/atomic"
	"time"
)

// startElection
func (rf *Raft) leaderElection() {

	voteCh := make(chan struct{})
	voteComplete := make(chan struct{})
	electionTimer := time.After(electionTimeOut())

	rf.mu.Lock()
	if !rf.staleState {
		// important, leaderElection may block at mutex because in follower state
		// it recieves a RPC, if it become stale then leaderElection should abort
		// otherwise we have a candidate with false term!

		rf.convertToCandidate()

		lastIdx := rf.getLastLogIndex()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
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
	case <-rf.staleSignal:
		// if it hear from a new leader from the same term, it should also go here
		// convert to follower mode
		DPrintf("%d recieves stale signal at term: %d, convert to follower", rf.id, rf.currentTerm)
		go rf.listen()
	case <-voteCh:
		DPrintf("id %d elected, term: %d", rf.id, rf.currentTerm)
		voteCh = nil //gc go to work!
		close(voteComplete)
		rf.mu.Lock()
		if rf.staleState {
			go rf.listen()
			rf.mu.Unlock()
			return
		}
		rf.convertToLeader()
		rf.mu.Unlock()
		go rf.broadCast(true)
		// convert to leader mode
	case <-electionTimer:
		//restart election
		voteCh = nil //gc go to work!
		close(voteComplete)
		go rf.leaderElection()
	}
}

func (rf *Raft) election(args *RequestVoteArgs, server int, voteCh chan struct{}, voteComplete chan struct{}, countVote *int32) {
	var reply RequestVoteReply
	DPrintf("%d sending requestVote to: %d, term: %d", rf.id, server, rf.currentTerm)
	if rf.callRequestVote(server, args, &reply) { //blocking call
		rf.mu.Lock()
		if reply.Term > rf.currentTerm { // RPC reply has larger term
			DPrintf("id: %d term smaller than rpc reply, term: %d", rf.id, rf.currentTerm)
			if rf.currentTerm == args.Term { // someone else haven't signal stale yet
				rf.convertToFollower(reply.Term)
				rf.stale()
			} else {
				rf.convertToFollower(reply.Term)
			}
			rf.mu.Unlock()
			return
		}
		// RPC response may come after candidate starts a new election
		if rf.state != CANDIDATE || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// only candidate can do the following operations!
		if reply.VoteGranted {
			atomic.AddInt32(countVote, 1)
			DPrintf("id：%d, countVote: %d, term: %d", rf.id, *countVote, rf.currentTerm)
		}
		if atomic.LoadInt32(countVote) > int32(len(rf.peers)/2) {
			//do something to convert to leader but prevent converting multiple times
			sendWithCancellation(voteCh, voteComplete)
		}
	}
}
