package conc

import (
	"context"
	"sync"
)

// Exercise 6 — fan-in
//
// Merge forwards every value from all input channels onto one output
// channel, and closes the output once ALL inputs are closed. If ctx is
// cancelled, it stops and closes the output (no leaked goroutines).
func Merge[T any](ctx context.Context, ins ...<-chan T) <-chan T {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	out := make(chan T)

	for _, in := range ins {
		wg.Go(func() {
			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
