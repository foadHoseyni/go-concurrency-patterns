package syncx

import (
	"errors"
	"sync"
)

// Exercise 6 — pub/sub where a slow subscriber can't block anyone
//
// A Broker fans each published message out to all current subscribers.
//
//	Subscribe(buffer)  returns a channel with that buffer size, plus an
//	                   unsubscribe func. After unsubscribe, the channel is
//	                   closed and gets no more messages. Calling unsubscribe
//	                   twice is safe.
//	Publish(msg)       must NEVER block: if a subscriber's buffer is full,
//	                   the message is dropped for that subscriber only, and
//	                   counted.
//	Dropped()          total number of dropped deliveries.
//	Close()            closes every subscriber channel. Publish after Close
//	                   returns ErrBrokerClosed. Safe to call twice.
//
// Key tools: a mutex around the subscriber set, and `select` with `default`
// for a non-blocking send. Watch out: never send on a channel that another
// goroutine may have just closed.
var ErrBrokerClosed = errors.New("broker closed")

type Broker struct {
	mu      sync.Mutex
	subs    map[chan string]struct{} // the set of current subscriber channels
	closed  bool
	dropped int
}

func NewBroker() *Broker {

	return &Broker{subs: make(map[chan string]struct{})}
}

func (b *Broker) Subscribe(buffer int) (<-chan string, func()) {
	ch := make(chan string, buffer)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		close(ch)
		return ch, func() {}
	}

	b.subs[ch] = struct{}{}
	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subs[ch]; ok {
			close(ch)
			delete(b.subs, ch)
		}

	}
	b.mu.Unlock()
	return ch, unsubscribe
}

func (b *Broker) Publish(msg string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}
	for ch, _ := range b.subs {
		select {
		case ch <- msg:
		default:
			b.dropped++
		}
	}
	return nil
}

func (b *Broker) Dropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for ch := range b.subs {
		close(ch)
	}
	clear(b.subs)
}
