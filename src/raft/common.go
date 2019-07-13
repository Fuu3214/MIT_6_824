package raft

import (
	"log"
	"math"
)

// Debugging
const (
	DebugCommon    = 0
	DebugElection  = 0
	DebugAgreement = 0
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugCommon > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfElection(format string, a ...interface{}) (n int, err error) {
	if DebugElection > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfAgreement(format string, a ...interface{}) (n int, err error) {
	if DebugAgreement > 0 {
		log.Printf(format, a...)
	}
	return
}

type serverState int

const (
	LEADER    serverState = 0
	FOLLOWER  serverState = 1
	CANDIDATE serverState = 2
	NULL      int         = -2333
	MAXLOGLEN int         = 1000
)

func Min(a int, b int) int {
	return int(math.Min(float64(a), float64(b)))
}
