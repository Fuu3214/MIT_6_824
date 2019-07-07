package raft

import "time"

func (rf *Raft) listen() {

	electionTimer := time.NewTimer(electionTimeOut())
	defer electionTimer.Stop()
	rf.mu.Lock()
	// before FOLLOWER state we may still receive rpc saying that we are stale,
	// hence there can be goroutines trying to write to rf.staleSignal
	// we should first clear them off
	// Also we need to ensure no one writes to rf.staleSignal in FOLLOWER state
	// otherwise we will switch to FOLLOWER immediately without sening leaderElection
	rf.unStale()
	rf.mu.Unlock()
	for {
		resetTimer(electionTimer, electionTimeOut())
		select {
		case <-rf.killCh: //terminate
			return
		case <-rf.heartBeatSignal:
			// heartBeat reached, restart election timeout
			DPrintf("FOLLOWER %d has recieved heartbeat signal, term: %d", rf.id, rf.currentTerm)
		case <-electionTimer.C:
			//restart election
			DPrintf("FOLLOWER %d has waited too long, convert to candidate, term: %d", rf.id, rf.currentTerm)
			go rf.leaderElection()
			return
		}
	}
}
