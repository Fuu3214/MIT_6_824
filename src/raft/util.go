package raft

// specify a done channel for cancellation, ensures that only one goroutine that
// sends to ch can be effective
func sendWithCancellation(ch chan struct{}, done chan struct{}) {
	// for { // seems meaningless
	// 	select {
	// 	case <-done:
	// 		return
	// 	default:
	// 		select {
	// 		case ch <- struct{}{}:
	// 		default:
	// 		}
	// 	}
	// }
	select {
	case <-ch: // consume anything in channel if exists, hopefully we reduce number of waiting goroutines
	default:
	}

	select {
	case <-done: // abort
	case ch <- struct{}{}: // send
	}
}

func send(ch chan struct{}) { //send a signal so that some one does not block
	select {
	case <-ch: // consume anything in channel if exists
	default:
	}
	ch <- struct{}{}
}
