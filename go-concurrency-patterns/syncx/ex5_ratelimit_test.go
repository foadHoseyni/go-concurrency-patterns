package syncx

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiter_BurstThenSteady(t *testing.T) {
	l := NewLimiter(3, 60*time.Millisecond)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if e := time.Since(start); e > 30*time.Millisecond {
		t.Errorf("first 3 (the burst) took %v, want immediate", e)
	}

	start = time.Now()
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if e := time.Since(start); e < 30*time.Millisecond || e > 200*time.Millisecond {
		t.Errorf("4th Wait took %v, want about one interval (60ms)", e)
	}
}

func TestLimiter_SteadyRate(t *testing.T) {
	l := NewLimiter(1, 20*time.Millisecond)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 6; i++ { // 1 from the full bucket + 5 refills
		if err := l.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if e := time.Since(start); e < 80*time.Millisecond || e > 400*time.Millisecond {
		t.Errorf("6 Waits at 1 per 20ms took %v, want ~100ms", e)
	}
}

func TestLimiter_Cancel(t *testing.T) {
	l := NewLimiter(1, time.Hour)
	if err := l.Wait(context.Background()); err != nil { // empties the bucket
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var err error
	within(t, time.Second, "Wait with a 30ms deadline", func() { err = l.Wait(ctx) })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}
