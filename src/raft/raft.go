package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"sync"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	id       int // this peer's index into peers[]
	leaderID int
	state    serverState
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent states
	currentTerm int
	votedFor    int
	log         []RaftLog

	//volatile states
	commitIndex int
	lastApplied int

	//volatile states for leaders
	nextIndex  []int
	matchIndex []int

	//channel for communication
	heartBeatSignal chan struct{}
	staleSignal     chan struct{}
	// doneStaleSignal chan struct{} // ensure only one signal can be effective
	staleState bool
}

// type heartBeatMsg struct {
// 	term     int
// 	serverID int
// }

type RaftLog struct {
	Term int
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER)

	return term, isleader
}

func (rf *Raft) setLog(idx int, value *RaftLog) {
	rf.log[idx] = *value
}
func (rf *Raft) getLog(idx int) *RaftLog {
	return &rf.log[idx]
}
func (rf *Raft) getLogs(fromIdx int, toIdx int) []RaftLog {
	return rf.log[fromIdx:toIdx]
}
func (rf *Raft) logLen() int {
	return len(rf.log)
}
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}
func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}
func (rf *Raft) numServer() int {
	return len(rf.peers)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, id int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.id = id

	rf.leaderID = NULL
	rf.currentTerm = 0

	rf.log = make([]RaftLog, 0)
	rf.log = append(rf.log, RaftLog{Term: 0}) // first log idx is 1

	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.state = FOLLOWER
	rf.votedFor = NULL

	rf.heartBeatSignal = make(chan struct{})
	rf.staleSignal = make(chan struct{})
	// rf.doneStaleSignal = make(chan struct{})
	rf.staleState = false
	rf.convertToFollower(0)

	go rf.listen()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// state conversion
func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = NULL
	DPrintf("id: %d convertToFollower, term: %d", rf.id, rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.id
	DPrintf("id: %d convertToCandidate, term: %d", rf.id, rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	DPrintf("id: %d convertToLeader, term %d", rf.id, rf.currentTerm)
	rf.state = LEADER
	rf.nextIndex = make([]int, rf.numServer())
	rf.matchIndex = make([]int, rf.numServer())
	for i := 0; i < rf.numServer(); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}

func (rf *Raft) stale() {
	if rf.staleState == false {
		rf.staleState = true
		go send(rf.staleSignal) // only one signal can be effective (hopefully)
		// go sendWithCancellation(rf.staleSignal, rf.doneStaleSignal) // Must call cancel in FOLLOWER state
	} else {
		rf.staleState = true
	}
}

func (rf *Raft) unStale() {
	rf.staleState = false
	// consume(rf.staleSignal) //consume if any
	// rf.doneStaleSignal = make(chan struct{})
}
