package syncx

import (
	"context"
	"sync"
	"time"
)

// Exercise 5 — a token-bucket rate limiter
//
// A Limiter allows short bursts but a steady average rate (e.g. "at most
// 10 calls per second to the payment provider, bursts of 3 allowed").
//
//   - The bucket holds at most `burst` tokens and starts FULL.
//   - One token is added every `interval` (never more than burst).
//   - Wait(ctx) takes one token. If none is available, it waits until one
//     is. If ctx is cancelled first, it returns ctx.Err() without a token.
//
// Two designs are possible: a background goroutine with a time.Ticker
// filling a buffered channel, or no goroutine at all — a mutex plus "how many
// tokens have accumulated since the last time?" arithmetic. Pick one and be
// ready to explain the trade-off.
type Limiter struct {
	mu       sync.Mutex
	burst    int
	interval time.Duration
	tokens   float64   // can be fractional: half a token has dripped in
	last     time.Time // when we last updated tokens
}

func NewLimiter(burst int, interval time.Duration) *Limiter {

	return &Limiter{
		burst:    burst,
		interval: interval,
		tokens:   float64(burst),
		last:     time.Now(),
	}
}

func (l *Limiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()

		l.tokens += float64(time.Since(l.last)) / float64(l.interval)
		l.tokens = min(l.tokens, float64(l.burst))
		l.last = time.Now()

		if l.tokens >= 1 {
			l.tokens -= 1
			l.mu.Unlock()
			return nil
		} else {
			l.mu.Unlock()
			select {
			case <-time.After(l.interval):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
