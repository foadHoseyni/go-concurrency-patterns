package conc

import (
	"context"
)

// Exercise 5 — run tasks concurrently, stop everything on the first error
//
// FirstError runs all tasks at the same time, each with a context derived
// from ctx. As soon as one task returns an error, the context passed to the
// others is cancelled. FirstError returns that first error (or nil if all
// succeed), but only AFTER every task has returned.
// (This is what golang.org/x/sync/errgroup does — write it yourself here.)
func FirstError(ctx context.Context, tasks ...func(ctx context.Context) error) error {
	ch := make(chan error, len(tasks))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, task := range tasks {
		go func() {
			ch <- task(ctx)
		}()
	}

	var first error
	for range len(tasks) {
		if err := <-ch; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}
