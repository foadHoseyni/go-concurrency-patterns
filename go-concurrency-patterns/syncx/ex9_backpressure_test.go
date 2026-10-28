package syncx

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBuffer_TryPutShedsWhenFull(t *testing.T) {
	b := NewBuffer(2)
	if err := b.TryPut(1); err != nil {
		t.Fatal(err)
	}
	if err := b.TryPut(2); err != nil {
		t.Fatal(err)
	}
	var err error
	within(t, 100*time.Millisecond, "TryPut on a full buffer", func() { err = b.TryPut(3) })
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("err = %v, want ErrBufferFull", err)
	}
	if n := b.Len(); n != 2 {
		t.Errorf("Len = %d, want 2", n)
	}
}

func TestBuffer_PutWaitsForRoom(t *testing.T) {
	b := NewBuffer(1)
	ctx := context.Background()
	if err := b.Put(ctx, 1); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(60 * time.Millisecond)
		b.Get(ctx) // frees the slot
	}()

	start := time.Now()
	if err := b.Put(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if e := time.Since(start); e < 40*time.Millisecond {
		t.Errorf("Put on a full buffer returned after %v; it should wait for room", e)
	}
	if v, _ := b.Get(ctx); v != 2 {
		t.Errorf("Get = %d, want 2", v)
	}
}

func TestBuffer_PutCancel(t *testing.T) {
	b := NewBuffer(1)
	b.Put(context.Background(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var err error
	within(t, time.Second, "Put on a full buffer with a deadline", func() { err = b.Put(ctx, 2) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if n := b.Len(); n != 1 {
		t.Errorf("Len = %d, want 1 (a cancelled Put must not add anything)", n)
	}
}

func TestBuffer_GetCancel(t *testing.T) {
	b := NewBuffer(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var err error
	within(t, time.Second, "Get on an empty buffer with a deadline", func() { _, err = b.Get(ctx) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

// A fast producer and a slow consumer: the producer is slowed down to the
// consumer's pace, items arrive in order, and the buffer never overflows.
func TestBuffer_ProducerSlowedToConsumerPace(t *testing.T) {
	b := NewBuffer(3)
	ctx := context.Background()
	const n = 10

	type result struct {
		took    time.Duration
		err     error
		maxSeen int
	}
	done := make(chan result, 1) // buffered: the producer never blocks on reporting
	go func() {
		var r result
		start := time.Now()
		for i := 0; i < n; i++ {
			if err := b.Put(ctx, i); err != nil {
				r.err = err
				break
			}
			r.maxSeen = max(r.maxSeen, b.Len())
		}
		r.took = time.Since(start)
		done <- r
	}()

	for i := 0; i < n; i++ {
		time.Sleep(10 * time.Millisecond) // slow consumer
		v, err := b.Get(ctx)
		if err != nil || v != i {
			t.Fatalf("Get = %d, %v; want %d", v, err, i)
		}
	}
	r := <-done
	if r.err != nil {
		t.Fatalf("Put: %v", r.err)
	}
	if r.maxSeen > 3 {
		t.Errorf("buffer held %d items, capacity is 3", r.maxSeen)
	}
	if r.took < 50*time.Millisecond {
		t.Errorf("producer finished in %v; with a full buffer it should have been held back to the consumer's pace", r.took)
	}
}
