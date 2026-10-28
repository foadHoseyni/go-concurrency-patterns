package syncx

import (
	"context"
)

// Exercise 3 — a semaphore: at most N at a time
//
// A Semaphore limits how many goroutines can be inside a section at once
// (e.g. at most 10 concurrent calls to a downstream service).
//
//	Acquire(ctx)  takes a slot, waiting if all N are taken. If ctx is
//	              cancelled while waiting, it gives up and returns ctx.Err()
//	              WITHOUT taking a slot.
//	Release()     gives a slot back.
//
// Hint: a buffered channel of capacity N is a semaphore.
type Semaphore struct {
	ch chan struct{}
}

func NewSemaphore(n int) *Semaphore {

	return &Semaphore{ch: make(chan struct{}, n)}
}

func (s *Semaphore) Acquire(ctx context.Context) error {

	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

}

func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
		panic("semaphore: release without acquire")
	}
}
