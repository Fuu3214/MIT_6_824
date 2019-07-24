package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"raft"
	"time"
)

const (
	RETRYNUM    int           = 30
	RPCINTERVAL time.Duration = 50 * time.Millisecond
)

type Clerk struct {
	CID          int64
	SEQ          int
	recentLeader int
	servers      []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.recentLeader = raft.NULL
	// You'll have to add code here.
	ck.CID = nrand()
	ck.SEQ = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) prepare() (int64, int, int) {
	var serverID int
	if ck.recentLeader == NULL {
		serverID = int(nrand()) % len(ck.servers)
	} else {
		serverID = ck.recentLeader
	}
	ck.SEQ++
	return ck.CID, ck.SEQ, serverID
}

func (ck *Clerk) Get(key string) string {
	CID, SEQ, serverID := ck.prepare()
	cnt := 0
	for {
		cnt++
		DPrintfKV("client: %d called Get to server: %d, seq: %d, cnt: %d", CID, serverID, SEQ, cnt)
		args := GetArgs{
			Key: key,
			CID: CID,
			SEQ: SEQ,
		}
		var reply GetReply

		if !ck.servers[serverID].Call("KVServer.Get", &args, &reply) { // block here
			if cnt > RETRYNUM {
				// probably dead
				serverID = (serverID + 1) % len(ck.servers)
				cnt = 0
			}
		} else {
			// DPrintfKV("client: %v, GET call, recieved reply: %v", CID, reply)
			if reply.WrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				cnt = 0
			} else {
				ck.recentLeader = serverID
				if reply.Err == OK || reply.Err == ErrDuplicate {
					return reply.Value
				}
				if reply.Err == ErrTimeOut {
					serverID = (serverID + 1) % len(ck.servers)
					cnt = 0
				}
				if reply.Err == ErrNoKey {
					return ""
				}
			}
		}
		time.Sleep(RPCINTERVAL)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, command KVCmd) {
	// You will have to modify this function.
	CID, SEQ, serverID := ck.prepare()
	cnt := 0
	for {
		cnt++
		if cnt == 1 {
			DPrintfKV("client: %d called PutAppend to server: %d, seq: %d, cnt: %d, OP: %v K: %v, V: %v", CID, serverID, SEQ, cnt, command, key, value)
		}
		args := PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    command,
			CID:   CID,
			SEQ:   SEQ,
		}
		var reply PutAppendReply

		if !ck.servers[serverID].Call("KVServer.PutAppend", &args, &reply) { // block here
			if cnt > RETRYNUM {
				// probably dead
				serverID = (serverID + 1) % len(ck.servers)
				cnt = 0
			}
		} else {
			// DPrintfKV("client: %v, %v call, recieved reply: %v", CID, command, reply)
			if reply.WrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				cnt = 0
			} else {
				ck.recentLeader = serverID
				if reply.Err == OK {
					return
				}
				if reply.Err == ErrTimeOut {
					serverID = (serverID + 1) % len(ck.servers)
					cnt = 0
				}
			}
		}
		time.Sleep(RPCINTERVAL)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
