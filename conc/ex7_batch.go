package conc

import (
	"context"
	"time"
)

// Exercise 7 — batching with size OR time limit (select + timer)
//
// Batch groups values from in into slices of at most `size` elements.
// A batch is sent as soon as it is full, OR when maxWait has passed since
// the FIRST value of that batch arrived — whichever comes first.
// When in is closed, any partial batch is sent, then the output is closed.
// Never send an empty batch. Stop and close the output if ctx is cancelled.
//
// This is how loggers, metrics clients and Kafka producers send data.
func Batch[T any](ctx context.Context, in <-chan T, size int, maxWait time.Duration) <-chan []T {
	ch := make(chan []T)

	go func() {
		defer close(ch)
		var batch []T
		var timer <-chan time.Time

		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			select {
			case ch <- batch:
			case <-ctx.Done():
				return false
			}
			batch = nil
			timer = nil
			return true
		}
		for {
			select {
			case v, ok := <-in:
				if !ok {
					flush()
					return
				}
				if len(batch) == 0 {
					timer = time.After(maxWait)
				}
				batch = append(batch, v)
				if len(batch) == size && !flush() {
					return
				}
			case <-timer:
				if !flush() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
