package conc

import (
	"errors"
	"maps"
	"sync"
)

// Exercise 8 — share memory by communicating (the actor pattern)
//
// Build a truck-load tracker WITHOUT sync.Mutex. One goroutine owns the
// map of loads; every method sends a request to it over a channel and waits
// for the reply. Because only that goroutine touches the map, there is no
// data race. (sync.Once for Close is fine.)
//
//	Load(truckID, n): add n items. ErrUnknownTruck if no such truck,
//	                  ErrFull if it would exceed capacity (then nothing changes).
//	Free(truckID):    remaining capacity.
//	Close():          stop the goroutine. Safe to call more than once.
//	                  After Close, Load and Free return ErrClosed (no panic,
//	                  no hang).

var (
	ErrUnknownTruck = errors.New("unknown truck")
	ErrFull         = errors.New("truck full")
	ErrClosed       = errors.New("tracker closed")
)

// request is one question to the owner goroutine. n == 0 means "just tell me
// how much is free". Each request carries its own channel for the answer.
type request struct {
	truck string
	n     int
	reply chan response
}

type response struct {
	free int
	err  error
}

type Tracker struct {
	reqs chan request  // methods -> owner
	done chan struct{} // closed by Close; tells everyone to stop
	once sync.Once     // makes Close safe to call twice
}

func NewTracker(capacity map[string]int) *Tracker {
	t := &Tracker{reqs: make(chan request), done: make(chan struct{})}

	free := maps.Clone(capacity)

	go func() { // the owner ("actor"): handles one request at a time
		for {
			select {
			case r := <-t.reqs:
				f, ok := free[r.truck]
				if !ok {
					r.reply <- response{err: ErrUnknownTruck}
				}
				if f > free[r.truck] {
					r.reply <- response{err: ErrFull}
				}
				free[r.truck] = f - r.n
				r.reply <- response{free: free[r.truck], err: nil}

			case <-t.done:
				return // Close was called: stop, no leak
			}
		}
	}()
	return t
}

// do sends a request to the owner and waits for the answer,
// or returns ErrClosed if the tracker has been closed.
func (t *Tracker) do(truck string, n int) response {
	r := request{truck: truck, n: n, reply: make(chan response, 1)}
	select {
	case t.reqs <- r:
		return <-r.reply
	case <-t.done:
		return response{err: ErrClosed}
	}
}

func (t *Tracker) Load(truckID string, n int) error {
	return t.do(truckID, n).err
}

func (t *Tracker) Free(truckID string) (int, error) {
	r := t.do(truckID, 0)
	return r.free, r.err
}

func (t *Tracker) Close() {
	t.once.Do(func() { close(t.done) })
}
