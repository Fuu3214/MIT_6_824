package raftkv

import "log"

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrFailure         = "ErrFailure"
	ErrTimeOut         = "ErrTimeOut"
	ErrCommandNotValid = "ErrCommandNotValid"
	ErrNotExecuted     = "ErrNotExecuted"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrDuplicate       = "ErrDuplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    KVCmd // "Put" or "Append"
	CID   int64
	SEQ   int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	CID int64
	SEQ int
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

const DebugKV = 0
const DebugCommon = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugCommon > 0 {
		log.Printf(format, a...)
	}
	return
}
func DPrintfKV(format string, a ...interface{}) (n int, err error) {
	if DebugKV > 0 {
		log.Printf(format, a...)
	}
	return
}
