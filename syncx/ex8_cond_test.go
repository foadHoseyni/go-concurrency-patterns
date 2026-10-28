package syncx

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestQueue_FIFO(t *testing.T) {
	q := NewQueue()
	for i := 1; i <= 3; i++ {
		if err := q.Push(i); err != nil {
			t.Fatal(err)
		}
	}
	for want := 1; want <= 3; want++ {
		if got, err := q.Pop(); err != nil || got != want {
			t.Errorf("Pop = %d, %v; want %d", got, err, want)
		}
	}
}

func TestQueue_PopWaitsForPush(t *testing.T) {
	q := NewQueue()
	got := make(chan int, 1)
	go func() {
		v, _ := q.Pop()
		got <- v
	}()

	select {
	case v := <-got:
		t.Fatalf("Pop returned %d from an empty queue; it should wait", v)
	case <-time.After(50 * time.Millisecond):
	}

	q.Push(42)
	select {
	case v := <-got:
		if v != 42 {
			t.Errorf("Pop = %d, want 42", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not wake up after Push")
	}
}

// Run with -race: every pushed item is popped exactly once.
func TestQueue_Concurrent(t *testing.T) {
	q := NewQueue()
	const n = 1000
	var mu sync.Mutex
	var popped []int
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v, err := q.Pop()
				if err != nil {
					return
				}
				mu.Lock()
				popped = append(popped, v)
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < n; i++ {
		q.Push(i)
	}
	q.Close()
	within(t, 2*time.Second, "consumers finishing after Close", wg.Wait)

	slices.Sort(popped)
	if len(popped) != n {
		t.Fatalf("popped %d items, want %d", len(popped), n)
	}
	for i, v := range popped {
		if v != i {
			t.Fatalf("item %d missing or duplicated", i)
		}
	}
}

func TestQueue_CloseWakesAllWaiters(t *testing.T) {
	q := NewQueue()
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := q.Pop()
			errs <- err
		}()
	}
	time.Sleep(30 * time.Millisecond) // let them all start waiting
	q.Close()
	q.Close() // twice must be safe

	within(t, time.Second, "waiting Pops after Close", wg.Wait)
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrQueueClosed) {
			t.Errorf("Pop err = %v, want ErrQueueClosed", err)
		}
	}
	if err := q.Push(1); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Push after Close: err = %v, want ErrQueueClosed", err)
	}
}

func TestQueue_DrainAfterClose(t *testing.T) {
	q := NewQueue()
	q.Push(1)
	q.Push(2)
	q.Close()
	var got []int
	within(t, time.Second, "Pop after Close", func() {
		for {
			v, err := q.Pop()
			if err != nil {
				return
			}
			got = append(got, v)
		}
	})
	if !slices.Equal(got, []int{1, 2}) {
		t.Errorf("got %v, want [1 2]: items left at Close are still delivered", got)
	}
}
