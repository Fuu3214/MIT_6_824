package raftkv

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type KVCmd string

const (
	PUT             KVCmd         = "PUT"
	GET             KVCmd         = "GET"
	APPEND          KVCmd         = "APPEND"
	NULL            int           = raft.NULL
	responseTimeOut time.Duration = 1 * time.Second // 3 second time out for any RPC
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command KVCmd
	Key     string
	Value   string
	CID     int64
	SEQ     int
}

type message struct { // data type that listenAndApply sends to RPC handlers
	op    *Op
	value string
	err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// index => channel of message, applytask() will write to corresponding channel of each index(Command)
	taskMapping map[int]chan message
	seqMapping  map[int64]int // CID => SEQ

	db *KVDatabase

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killCh chan struct{}
}

// Get ...
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Command: GET,
		Key:     args.Key,
		Value:   "",
		CID:     args.CID,
		SEQ:     args.SEQ,
	}
	DPrintfKV("id: %d, recieved Get, op: %v", kv.rf.GetID(), op)
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.subscribe(index)
	select { // should block here, I suppose
	case msg := <-ch:
		DPrintfKV("id: %d, Get signaled, msg.op: %v, msg.val: %v, msg.err: %v", kv.rf.GetID(), msg.op, msg.value, msg.err)
		if !opIsEqual(&op, msg.op) {
			// state machine applied some different command at index
			reply.WrongLeader = true
			kv.unSubscribe(index)
			return
		}

		reply.WrongLeader = false
		reply.Value = msg.value
		reply.Err = msg.err
	case <-time.After(responseTimeOut):
		reply.Err = ErrTimeOut
	case <-kv.killCh:
	}
	kv.unSubscribe(index)
}

// PutAppend ...
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	op := Op{
		Command: args.Op,
		Key:     args.Key,
		Value:   args.Value,
		CID:     args.CID,
		SEQ:     args.SEQ,
	}
	DPrintfKV("id: %d, recieved PutAppend, op: %v", kv.rf.GetID(), op)
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := kv.subscribe(index)
	select { // should block here, I suppose
	case msg := <-ch:
		DPrintfKV("id: %d, put append signaled, msg: %v", kv.rf.GetID(), msg)
		if !opIsEqual(&op, msg.op) {
			// state machine applied some different command at index
			reply.WrongLeader = true
			kv.unSubscribe(index)
			return
		}

		reply.WrongLeader = false
		reply.Err = msg.err
	case <-time.After(responseTimeOut):
		reply.Err = ErrTimeOut
	case <-kv.killCh:
	}
	kv.unSubscribe(index)
}

// listen to applyCh, if any command then apply to DB and send message to corresponding channel
func (kv *KVServer) listenAndApply() {
	for {
		select {
		case msg := <-kv.applyCh:
			DPrintfKV("id: %d, listenAndApply(), msg recieved, msg: %v", kv.rf.GetID(), msg)
			op, ok := msg.Command.(Op)
			index := msg.CommandIndex
			var result message
			result.op = &op
			result.err = OK
			if !msg.CommandValid || !ok {
				result.err = ErrCommandNotValid
				go kv.publish(index, &result)
				continue
			}

			kv.mu.Lock()
			maxSeq, ok := kv.seqMapping[op.CID]
			kv.mu.Unlock()
			DPrintfKV("op.SEQ: %d, maxSeq: %d", op.SEQ, maxSeq)
			if op.SEQ <= maxSeq && op.Command != GET {
				if op.Command == GET {
					result.err = ErrDuplicate
				}
				go kv.publish(index, &result)
				continue
			}
			kv.seqMapping[op.CID] = op.SEQ

			success := false
			switch op.Command {
			case GET:
				result.value, success = kv.db.Get(op.Key) // blocking
				if !success {
					result.err = ErrNoKey
				}
			case PUT:
				success = kv.db.Put(op.Key, op.Value) // blocking
			case APPEND:
				success = kv.db.Append(op.Key, op.Value) // blocking

			default:
				result.err = ErrCommandNotValid
			}
			if success {
				result.err = OK
			}
			go kv.publish(index, &result)
		case <-kv.killCh:
			return
		}
	}
}

// publisher and subscriber pattern
func (kv *KVServer) subscribe(index int) chan message {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.taskMapping[index]; ok {
		return ch
	} else {
		kv.taskMapping[index] = make(chan message)
		return kv.taskMapping[index]
	}
}

func (kv *KVServer) publish(index int, msg *message) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.taskMapping[index]
	if ok {
		ch <- *msg
	}
}

func (kv *KVServer) unSubscribe(index int) {
	kv.mu.Lock()
	ch, ok := kv.taskMapping[index] // not exist
	if !ok {
		return
	}
	delete(kv.taskMapping, index) // unregister
	kv.mu.Unlock()
	if ok {
		close(ch)
	}
}

func opIsEqual(proposed *Op, executed *Op) bool {
	return proposed.CID == executed.CID &&
		proposed.Key == executed.Key &&
		proposed.Value == executed.Value &&
		proposed.SEQ == executed.SEQ
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = InitDB()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.taskMapping = make(map[int]chan message)
	kv.seqMapping = make(map[int64]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan struct{})

	go kv.listenAndApply()

	// You may need initialization code here.

	return kv
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.killCh)
	kv.mu.Lock()
	for index, ch := range kv.taskMapping {
		close(ch)
		delete(kv.taskMapping, index) // unregister
	}
	kv.mu.Unlock()
	// Your code here, if desired.
}
