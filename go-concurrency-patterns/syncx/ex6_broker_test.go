package syncx

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func drain(ch <-chan string) []string {
	var out []string
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, m)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
}

func TestBroker_SlowSubscriberDoesNotBlock(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	fast, _ := b.Subscribe(10)
	slow, _ := b.Subscribe(1) // nobody reads this one until the end

	within(t, time.Second, "5 Publish calls with a full subscriber", func() {
		for i := 1; i <= 5; i++ {
			if err := b.Publish(fmt.Sprint(i)); err != nil {
				t.Errorf("Publish: %v", err)
			}
		}
	})

	if got := drain(fast); len(got) != 5 {
		t.Errorf("fast subscriber got %v, want all 5", got)
	}
	if got := drain(slow); len(got) != 1 || got[0] != "1" {
		t.Errorf("slow subscriber got %v, want just [1] (buffer of 1)", got)
	}
	if d := b.Dropped(); d != 4 {
		t.Errorf("dropped = %d, want 4", d)
	}
}

func TestBroker_Unsubscribe(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ch, unsub := b.Subscribe(5)
	b.Publish("a")
	unsub()
	unsub() // twice must be safe
	if err := b.Publish("b"); err != nil {
		t.Fatalf("Publish after unsubscribe: %v", err)
	}
	got := drain(ch)
	if len(got) != 1 || got[0] != "a" {
		t.Errorf("got %v, want [a] then a closed channel", got)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after unsubscribe")
		}
	default:
		t.Error("channel should be closed after unsubscribe (receive would block)")
	}
}

func TestBroker_Close(t *testing.T) {
	b := NewBroker()
	ch, unsub := b.Subscribe(1)
	b.Close()
	b.Close() // twice must be safe
	unsub()   // unsubscribe after Close must be safe too
	if _, ok := <-ch; ok {
		t.Error("subscriber channel should be closed by Close")
	}
	if err := b.Publish("x"); !errors.Is(err, ErrBrokerClosed) {
		t.Errorf("err = %v, want ErrBrokerClosed", err)
	}
}

// Run with -race: publishing, subscribing and unsubscribing all at once must
// never panic (send on closed channel) or race.
func TestBroker_Concurrent(t *testing.T) {
	b := NewBroker()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				b.Publish("m")
			}
		}()
		go func() {
			defer wg.Done()
			ch, unsub := b.Subscribe(2)
			drain(ch)
			unsub()
		}()
	}
	wg.Wait()
	b.Close()
}
