package conc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestFirstError_AllSucceed(t *testing.T) {
	ok := func(ctx context.Context) error { time.Sleep(10 * time.Millisecond); return nil }
	if err := FirstError(context.Background(), ok, ok, ok); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestFirstError_CancelsOthers(t *testing.T) {
	boom := errors.New("boom")
	var finished, sawCancel atomic.Int32

	slow := func(ctx context.Context) error {
		defer finished.Add(1)
		select {
		case <-ctx.Done():
			sawCancel.Add(1)
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	}
	failing := func(ctx context.Context) error {
		defer finished.Add(1)
		time.Sleep(20 * time.Millisecond)
		return boom
	}

	start := time.Now()
	err := FirstError(context.Background(), slow, failing, slow)

	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom (the FIRST error, not the cancellations)", err)
	}
	if e := time.Since(start); e > 500*time.Millisecond {
		t.Errorf("took %v; the slow tasks should be cancelled", e)
	}
	if n := sawCancel.Load(); n != 2 {
		t.Errorf("%d slow tasks saw cancellation, want 2", n)
	}
	if n := finished.Load(); n != 3 {
		t.Errorf("returned while %d of 3 tasks had finished; must wait for all", n)
	}
}
