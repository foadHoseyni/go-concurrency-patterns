# Go concurrency patterns

A combined practice set covering both halves of Go concurrency: channels and goroutines (`conc/`), and shared state with `sync` primitives (`syncx/`). Each exercise is a single file with a comment block describing what to build and a stub to fill in, plus a test file that checks correctness, races, and leaks.

```
go test -race -timeout 60s ./...                      # everything
go test -race ./conc/                                 # channels only
go test -race ./syncx/                                # sync only
go test -race -run FetchAll ./conc/                   # one exercise
go test -race -count=20 -run Processor ./syncx/       # repeat to shake out timing bugs
```

Work without AI, as on interview day. For each exercise, first read the comment block **and** the test file. Restate it in one sentence, name the pattern, then write the simplest correct version. Run with `-race`. If you're stuck for more than 20 to 25 minutes, write down how you'd approach it and move on.

## `conc/`: channels and goroutines

1. **`FetchAll`**: goroutines and `sync.WaitGroup`. Start one goroutine per id, and wait for all of them. The question to think about: how do you put each result in the right place without a data race?
2. **`Generate` and `Square`**: the pipeline pattern. Each stage owns its output channel and closes it when it's done, and only the *sender* closes. Then add cancellation: every send becomes a `select` between sending and `<-ctx.Done()`.
3. **`Pool`**: a worker pool. A fixed number of goroutines reads from one jobs channel. The test measures that *exactly* four jobs run at the same time and that results keep their order.
4. **`WithTimeout`**: `select` with `time.After`. The real lesson is the leak: if the timeout wins, `f` is still running, and its goroutine will try to send its result to a channel nobody reads.
5. **`FirstError`**: running tasks together and cancelling the rest when one fails. You'll need `context.WithCancel`, a `WaitGroup`, and a safe way to remember only the *first* error. This is what `errgroup` does.
6. **`Merge`**: fan-in. One goroutine per input, all forwarding to one output. The tricky part is *who* closes the output, and *when*.
7. **`Batch`**: one loop with a `select` over three things: a new value, a timer, and cancellation. The timer starts when the *first* value of a batch arrives, not at a fixed interval. Remember that receiving from a `nil` channel blocks forever.
8. **`Tracker`**: "share memory by communicating". One goroutine owns the map, and the methods send it requests over a channel with a reply channel inside. No mutex. `Close` must be safe to call twice.

## `syncx/`: shared state with `sync`

1. **`Stats`**: a counter map shared by many goroutines, protected with `sync.Mutex`. The catch is `Snapshot`: returning your internal map hands the caller a reference to state you protect with a lock.
2. **`Lazy`**: load something exactly once, even when 100 goroutines ask at the same moment. Try both `sync.Once` with fields and `sync.OnceValues`.
3. **`Semaphore`**: at most N goroutines in a section at the same time, with cancellation while waiting. A buffered channel of size N is all you need.
4. **`Group`** (singleflight): ten concurrent requests for the same key do the work once and share the result. You'll combine a mutex, a map of calls in progress, and a channel you close to wake every waiter at once.
5. **`Limiter`**: a token bucket that allows bursts with a steady average rate. Decide between a background goroutine with a ticker and pure arithmetic under a mutex, and be ready to explain why.
6. **`Broker`**: pub/sub where `Publish` never blocks. A full subscriber simply misses the message. The key tool is `select` with `default`, plus making sure you never send on a channel someone else may have just closed.
7. **`Processor`**: graceful shutdown. Stop accepting work, finish what was accepted, but give up when the deadline passes. The concurrency test fires `Submit` while `Shutdown` runs, and a naive version panics with "send on closed channel".

## Questions to answer out loud after each one

**Channels (`conc/`):**

- Who closes this channel, and why that goroutine?
- Could any goroutine here block forever? Under what conditions?
- What happens if the caller stops reading halfway?
- Would a mutex be simpler here? When would you choose one over the other?
- Buffered or unbuffered, and what changes if you switch?

**Locks (`syncx/`):**

- What exactly does this lock protect, and is every access to that state done while holding it?
- Is anything slow or blocking done while holding the lock? Could that stall everyone else?
- Could this be done with channels instead? Which version would you rather maintain?
- What happens if this method is called twice, or after Close or Shutdown?
- Mutex or `RWMutex`? Is there actually a read-heavy pattern that would benefit?

## Recent Go worth knowing (you're on 1.27)

`sync.WaitGroup.Go(func() { ... })` (Go 1.25) replaces the `Add(1)` / `go` / `defer Done()` pattern, and `go fix` now suggests it. `sync.OnceValue` and `sync.OnceValues` (Go 1.21) wrap a function so it runs once and returns its result. `testing/synctest` (stable since Go 1.25, with `synctest.Sleep` added in 1.27) runs tests with a fake clock.
