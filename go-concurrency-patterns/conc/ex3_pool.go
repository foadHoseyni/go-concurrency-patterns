package conc

import "sync"

// Exercise 3 — worker pool with bounded concurrency
//
// Pool runs fn on every job using at most `workers` goroutines at the same
// time, and returns the results in the same order as jobs.
// Typical shape: a jobs channel, `workers` goroutines reading from it, and a
// way to put each result in the right slot.
func Pool[J, R any](jobs []J, workers int, fn func(J) R) []R {
	result := make([]R, len(jobs))
	idx := make(chan int)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for j := range idx {
				result[j] = fn(jobs[j])
			}
		})
	}

	for i := range jobs {
		idx <- i // blocks until a worker is free, which is what limits concurrency
	}
	close(idx) // tells the workers there's no more work

	wg.Wait()
	return result
}
