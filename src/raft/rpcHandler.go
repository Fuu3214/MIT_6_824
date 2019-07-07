package raft

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("id: %d received requestVote from: %d, rpc term: %d, cur Term: %d", rf.id, args.CandidateID, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("id: %d, term smaller than rpc request", rf.id)
		rf.votedFor = NULL
		rf.stale(args.Term)
	}
	reply.Term = rf.currentTerm

	if (rf.votedFor == NULL || rf.votedFor == args.CandidateID) && logUpToDate(args, rf) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return
}

// logUpToDate returns true if the candidate has up-to-date log
func logUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	if args.LastLogTerm != rf.getLastLogTerm() {
		return args.LastLogTerm > rf.getLastLogTerm()
	} else {
		return args.LastLogIndex >= rf.getLastLogIndex()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) callRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

// AppendEntries handlers
//AppendEntriesArgs is arguement for AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries []RaftLog

	LeaderCommit int
}

//AppendEntriesReply is reply for AppendEntries
type AppendEntriesReply struct {
	// Your data here (2A).
	Term     int
	Succcess bool
}

// AppendEntries is an RPC call
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("%d recieved appendentry from: %d, but RPC term: %d is smaller than current term: %d, ignore!", rf.id, args.LeaderID, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Succcess = false
		return
	}

	if rf.state == LEADER && args.Term == rf.currentTerm { // 2 leaders
		DPrintf("Houston, we fucked up!")
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = args.LeaderID
	}
	if rf.state == FOLLOWER {
		rf.convertToFollower(args.Term)
		go func() {
			DPrintf("%d recieved appendentry from: %d, current state: %d, rpc term: %d, current term: %d, signal heartbeat", rf.id, args.LeaderID, rf.state, args.Term, rf.currentTerm)
			send(rf.heartBeatSignal) // signal follower to reset timer
		}()
	} else {
		// stale leader and candidate
		DPrintf("%d recieved appendentry from: %d, current state: %d, rpc term: %d, current term: %d, convert to follower", rf.id, args.LeaderID, rf.state, args.Term, rf.currentTerm)
		rf.stale(args.Term) // change to follower and signal control to ignore timeout
	}

	reply.Term = rf.currentTerm

	if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm { // Log inconsistant
		DPrintf("Log inconsistant")
		reply.Succcess = false
		return
	}

	idx := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		idx++
		if idx < rf.logLen() {
			if rf.getLog(idx).Term == args.Entries[i].Term {
				continue
			} else {
				rf.log = rf.log[:idx]
			}
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Succcess = true
	return
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}
