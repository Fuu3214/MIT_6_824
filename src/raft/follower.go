package raft

import "time"

func (rf *Raft) listen() {

	for {
		electionTimer := time.After(electionTimeOut())
		select {
		case <-rf.heartBeatSignal:
			// heartBeat reached, restart election timeout
			DPrintf("FOLLOWER %d has recieved heartbeat signal, term: %d", rf.id, rf.currentTerm)
		case <-electionTimer:
			//restart election
			DPrintf("FOLLOWER %d has waited too long, convert to candidate, term: %d", rf.id, rf.currentTerm)
			rf.mu.Lock()
			// in FOLLOWER state we may still receive rpc saying that we are stale,
			// hence there can be goroutines trying to write to rf.staleSignal
			// we should clear them off before we try to convert to CANDIDATE X
			// or ensure only one goroutine writes to rf.staleSignal *
			// otherwise we will switch to FOLLOWER immediately without sening leaderElection
			rf.unStale()
			rf.mu.Unlock()
			go rf.leaderElection()
			return
		}
	}
}
