package raft

import (
	"math/rand"
	"time"
)

const (
	HEARTBEAT         int           = 100
	ELECTIONTIMEOUT   int           = 300
	RAND              int           = 200
	KEEPALIVEINTERVAL time.Duration = 2 * time.Millisecond
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

func resetTimer(timer *time.Timer, time time.Duration) {
	// timer may be not active, and fired
	if !timer.Stop() {
		<-timer.C //try to drain from the channel
	}
	timer.Reset(time)
}
