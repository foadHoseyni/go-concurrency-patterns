package syncx

import (
	"context"
	"errors"
)

// Exercise 9 — backpressure and load shedding
//
// A Buffer sits between a fast producer and a slow consumer and holds at
// most `capacity` items. When it is full, the producer has two choices:
//
//	Put(ctx, v)   BACKPRESSURE: wait until there is room, so the producer is
//	              slowed down to the consumer's speed. If ctx is cancelled
//	              first, give up and return ctx.Err() (v is not added).
//	TryPut(v)     LOAD SHEDDING: never wait. If full, return ErrBufferFull
//	              immediately and drop v.
//	Get(ctx)      takes the oldest item, waiting while the buffer is empty;
//	              returns ctx.Err() if cancelled first.
//	Len()         how many items are in the buffer right now.
//
// The buffer must never hold more than `capacity` items — that bound is the
// whole point: without it, a slow consumer makes memory grow until the
// service falls over.
var ErrBufferFull = errors.New("buffer full")

type Buffer struct {
	ch chan int
}

func NewBuffer(capacity int) *Buffer {

	return &Buffer{ch: make(chan int, capacity)}
}

func (b *Buffer) Put(ctx context.Context, v int) error {
	select {
	case b.ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Buffer) TryPut(v int) error {
	select {
	case b.ch <- v:
		return nil
	default:
		return ErrBufferFull
	}
}

func (b *Buffer) Get(ctx context.Context) (int, error) {
	select {
	case v := <-b.ch:
		return v, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *Buffer) Len() int {

	return len(b.ch)
}
