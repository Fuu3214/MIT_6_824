package raftkv

import "time"

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
	// DPrintfKV("id: %d, recieved Get, op: %v", kv.rf.GetID(), op)
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.subscribe(index)
	select { // should block here, I suppose
	case <-kv.killCh:
	case msg := <-ch:
		DPrintfKV("id: %d, Get signaled, index: %d, msg.op: %v, msg.val: %v, msg.err: %v", kv.rf.GetID(), index, msg.op, msg.value, msg.err)
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
	// DPrintfKV("id: %d, recieved PutAppend, op: %v", kv.rf.GetID(), op)
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := kv.subscribe(index)
	select { // should block here, I suppose
	case <-kv.killCh:
	case msg := <-ch:
		// DPrintfKV("id: %d, put append signaled, index: %d, msg: %v", kv.rf.GetID(), index, msg)
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
	}
	kv.unSubscribe(index)
}
