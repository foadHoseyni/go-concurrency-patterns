package conc

import (
	"errors"
	"time"
)

// Exercise 4 — select with a timeout, without leaking
//
// WithTimeout runs f in a goroutine and returns its result, or ErrTimeout if
// f takes longer than d. When it times out, f keeps running in the
// background — make sure its goroutine can still finish and exit when f
// eventually returns (hint: what happens when it sends on an unbuffered
// channel nobody reads anymore?).
var ErrTimeout = errors.New("timeout")

func WithTimeout[T any](d time.Duration, f func() T) (T, error) {

	ch := make(chan T, 1)
	go func() {
		ch <- f()
	}()
	for {
		select {
		case v := <-ch:
			return v, nil
		case <-time.After(d):
			var zero T
			return zero, ErrTimeout
		}
	}

}
