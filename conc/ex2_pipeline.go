package conc

import (
	"context"
)

// Exercise 2 — generator + pipeline, closing channels, cancellation
//
// Generate sends each number on the returned channel, then closes it.
// Square reads numbers from in, sends their squares, and closes its output
// when in is closed.
//
// Both must stop (and close their output) if ctx is cancelled, even when
// nobody is reading anymore — otherwise their goroutine leaks.
func Generate(ctx context.Context, nums ...int) <-chan int {

	out := make(chan int)
	go func() {
		defer close(out)
		for _, num := range nums {
			select {
			case out <- num:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func Square(ctx context.Context, in <-chan int) <-chan int {

	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			select {
			case out <- n * n:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
