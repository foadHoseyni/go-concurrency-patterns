package syncx

import (
	"errors"
	"sync"
)

// Exercise 8 — sync.Cond: sleep until shared state changes
//
// A Queue is a FIFO queue whose Pop waits while the queue is empty.
// Build it with sync.Mutex + sync.Cond (not channels — that's the point).
//
//	Push(v)   adds v to the back. Returns ErrQueueClosed after Close.
//	Pop()     removes and returns the front item. If the queue is empty, it
//	          waits (without busy-looping) until an item arrives. After
//	          Close, it still returns the items that are left, and once the
//	          queue is empty it returns ErrQueueClosed instead of waiting.
//	Close()   wakes EVERY waiting Pop. Safe to call twice.
//
// Remember: Wait() must be called with the lock held, and the condition is
// always re-checked in a `for` loop, never an `if`.
var ErrQueueClosed = errors.New("queue closed")

type Queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []int
	closed bool
}

func NewQueue() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Push(v int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.items = append(q.items, v)

	q.cond.Signal()

	return nil
}

func (q *Queue) Pop() (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 && q.closed {
		return 0, ErrQueueClosed
	}

	v := q.items[0]
	q.items = q.items[1:]
	return v, nil
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
