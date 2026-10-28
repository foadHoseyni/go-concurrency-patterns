package syncx

import (
	"context"
	"errors"
	"sync"
)

// Exercise 7 — graceful shutdown
//
// A Processor runs a fixed number of workers that handle submitted jobs.
//
//	Submit(job)     queues a job. Returns ErrStopped once Shutdown has been
//	                called. Must never panic, even if called concurrently
//	                with Shutdown.
//	Shutdown(ctx)   stops accepting new jobs, then waits until every job that
//	                was already accepted has been handled, and returns nil.
//	                If ctx expires first, it cancels the context given to the
//	                handlers (so they can stop early) and returns ctx.Err()
//	                without waiting any longer.
//
// This is what a service does on SIGTERM in Kubernetes: stop taking traffic,
// finish what's in flight, but don't wait forever.
var ErrStopped = errors.New("processor stopped")

type Processor struct {
	mu      sync.RWMutex
	stopped bool
	jobs    chan int           // the queue the workers read from
	wg      sync.WaitGroup     // tracks the workers
	cancel  context.CancelFunc // cancels the context the handlers receive
}

// NewProcessor starts `workers` goroutines. Each handled job is passed to
// handle together with a context that is cancelled if Shutdown times out.
func NewProcessor(workers int, handle func(ctx context.Context, job int)) *Processor {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := make(chan int, workers)
	p := Processor{jobs: jobs, cancel: cancel}

	for range workers {

		p.wg.Go(func() {

			for j := range p.jobs {
				handle(ctx, j)
			}
		})
	}
	return &p
}

func (p *Processor) Submit(job int) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		return ErrStopped
	}
	p.jobs <- job

	return nil
}

func (p *Processor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		close(p.jobs)
	}
	p.mu.Unlock()

	done := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}

}
