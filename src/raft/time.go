package raft

import (
	"math/rand"
	"time"
)

const (
	HEARTBEAT       int = 100
	ELECTIONTIMEOUT int = 300
	RAND            int = 100
)

// timer
func heartBeat() time.Duration {
	return time.Duration(HEARTBEAT) * time.Millisecond
}

func electionTimeOut() time.Duration {
	randomTimeout := ELECTIONTIMEOUT + rand.Intn(RAND)
	electionTimeout := time.Duration(randomTimeout) * time.Millisecond
	return electionTimeout
}
