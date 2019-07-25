package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"sync"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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
	mu      sync.Mutex // Lock to protect shared access to this peer's state
	applyMu sync.Mutex // Lock to ensure shared access to applyindex

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	id    int // this peer's index into peers[]
	state serverState
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent states
	CurrentTerm int
	VotedFor    int
	Log         []RaftLog

	//volatile states
	commitIndex int
	lastApplied int

	//volatile states for leaders
	nextIndex  []int
	matchIndex []int

	isStale bool // if leader of candidate is stale

	//channel for communication
	heartBeatSignal chan struct{}
	staleSignal     chan struct{}
	doneStaleSignal chan struct{} // ensure only one signal can be effective
	killCh          chan struct{}

	applyCh chan ApplyMsg
}

// type heartBeatMsg struct {
// 	term     int
// 	serverID int
// }

type RaftLog struct {
	Term    int
	Command interface{}
}

// GetState returns CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = (rf.state == LEADER)

	return term, isleader
}

// GetLeaderID returns id of whom current server believes to be the leader
func (rf *Raft) GetLeaderID() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.VotedFor
}

func (rf *Raft) appendLog(newLog ...RaftLog) {
	rf.Log = append(rf.Log, newLog...)
	rf.persist()
}
func (rf *Raft) getLog(idx int) *RaftLog {
	return &rf.Log[idx]
}
func (rf *Raft) getLogs(fromIdx int, toIdx int) []RaftLog {
	return rf.Log[fromIdx:toIdx]
}
func (rf *Raft) logLen() int {
	return len(rf.Log)
}
func (rf *Raft) isLogBegining(idx int) bool {
	return rf.getLog(idx).Term == 0
}
func (rf *Raft) getLastLogIndex() int {
	return len(rf.Log) - 1
}
func (rf *Raft) getLastLogTerm() int {
	return rf.Log[rf.getLastLogIndex()].Term
}
func (rf *Raft) numServer() int {
	return len(rf.peers)
}

func (rf *Raft) GetID() int {
	return rf.id
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(&rf.CurrentTerm)
	e.Encode(&rf.VotedFor)
	e.Encode(&rf.Log)
	e.Encode(&rf.commitIndex)
	e.Encode(&rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//DPrintf("id: %d, persisted: %v, commitIndex: %d, lastapplied: %d", rf.id, rf.Log, rf.commitIndex, rf.lastApplied)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.CurrentTerm) != nil ||
		d.Decode(&rf.VotedFor) != nil ||
		d.Decode(&rf.Log) != nil ||
		d.Decode(&rf.commitIndex) != nil ||
		d.Decode(&rf.lastApplied) != nil {
		DPrintf("error reading persist state")
		rf.Kill()
	}
	DPrintf("id: %d, restored: %v, commitIndex: %d, lastapplied: %d", rf.id, rf.Log, rf.commitIndex, rf.lastApplied)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.CurrentTerm
	newLog := RaftLog{
		Term:    term,
		Command: command,
	}
	rf.appendLog(newLog)

	DPrintfAgreement("id: %d, state: %v, term: %d, new Log appended, index: %d", rf.id, rf.state, rf.CurrentTerm, index)
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.killCh)
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
	rf.id = id

	rf.killCh = make(chan struct{})
	rf.heartBeatSignal = make(chan struct{})
	rf.staleSignal = make(chan struct{})
	rf.doneStaleSignal = make(chan struct{})
	rf.isStale = false

	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.VotedFor = NULL
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.Log = make([]RaftLog, 0)
	rf.Log = append(rf.Log, RaftLog{Term: 0}) // first Log idx is 1

	// initialize from state persisted before a crash
	rf.persister = persister
	rf.readPersist(persister.ReadRaftState())

	go rf.listen()
	// Your initialization code here (2A, 2B, 2C).

	return rf
}

// state conversion
func (rf *Raft) convertToFollower(term int) {
	currentTerm := rf.CurrentTerm
	state := rf.state
	rf.CurrentTerm = term
	rf.state = FOLLOWER
	DPrintfElection("id: %d convertToFollower, term: %d", rf.id, rf.CurrentTerm)
	if currentTerm != term || state != FOLLOWER {
		rf.persist()
	}
}

func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.id
	DPrintfElection("id: %d convertToCandidate, term: %d", rf.id, rf.CurrentTerm)
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	DPrintfElection("id: %d convertToLeader, term %d", rf.id, rf.CurrentTerm)
	rf.state = LEADER
	rf.nextIndex = make([]int, rf.numServer())
	rf.matchIndex = make([]int, rf.numServer())
	for i := 0; i < rf.numServer(); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}

// stale wiil change rf to follower and signal control to ignore timeout
func (rf *Raft) stale() {
	// if rf.isStale == false {
	// 	rf.isStale = true
	// 	go send(rf.staleSignal) // only one signal can be effective (hopefully)
	// 	// go sendWithCancellation(rf.staleSignal, rf.doneStaleSignal) // Must call cancel in FOLLOWER state
	// } else {
	// 	rf.isStale = true
	// }

	//can only be effective when in FOLLOWER state

	rf.isStale = true
	sendWithCancellation(rf.staleSignal, rf.doneStaleSignal) // Must call cancel in FOLLOWER state

}

func (rf *Raft) unStale() {
	rf.isStale = false
	consume(rf.staleSignal) //consume if any
	close(rf.doneStaleSignal)
	rf.doneStaleSignal = make(chan struct{})
}

func (rf *Raft) applyLogs(commitIndex int) {
	// rf.applyMu.Lock()
	// defer rf.applyMu.Unlock()
	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		rf.lastApplied = i

		// rf.mu.Lock()
		command := rf.getLog(i).Command
		// rf.mu.Unlock()

		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      command,
		}
		DPrintfAgreement("id: %d, term: %d, applying msg: %v, Log: %v", rf.id, rf.CurrentTerm, msg, rf.Log)
		rf.applyCh <- msg
	}
}
