package raftkv

import (
	"bytes"
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
	responseTimeOut time.Duration = 1 * time.Second // 1 second time out for any RPC
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
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	// index => channel of message, applytask() will write to corresponding channel of each index(Command)
	taskMapping map[int]chan message
	seqMapping  map[int64]int // CID => SEQ

	db *KVDatabase

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killCh chan struct{}
}

// listen to applyCh, if any command then apply to DB and send message to corresponding channel
func (kv *KVServer) receiveApply() {
	for {
		select {
		case msg := <-kv.applyCh:
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
			DPrintfKV("id: %d, listenAndApply(), msg recieved, msg: %v, op.SEQ: %d, maxSeq: %d", kv.rf.GetID(), msg, op.SEQ, maxSeq)
			if op.SEQ <= maxSeq && op.Command != GET {
				kv.mu.Unlock()
				go kv.publish(index, &result)
				continue
			}
			if op.SEQ > maxSeq {
				kv.seqMapping[op.CID] = op.SEQ
			}
			kv.mu.Unlock()

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
			kv.persist()

			// DPrintfKV("ID: %d, Persisted, seqMapping: %v", kv.rf.GetID(), kv.seqMapping)
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
	ch, ok := kv.taskMapping[index]
	kv.mu.Unlock()
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

	kv.persister = persister
	kv.readPersist(persister.ReadSnapshot())

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan struct{})

	go kv.receiveApply()

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
	DPrintfKV(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	DPrintfKV("id: %d, seqmapping: %v", kv.rf.GetID(), kv.seqMapping)
	close(kv.killCh)
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// save kv server's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (kv *KVServer) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(&kv.seqMapping)
	e.Encode(&kv.db.Storage)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

//
// restore previously persisted state.
//
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.seqMapping) != nil ||
		d.Decode(&kv.db.Storage) != nil {
		DPrintfKV("error reading persist state")
		kv.Kill()
	}
}
