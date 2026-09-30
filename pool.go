package aergo

// Pool is a fixed-capacity free list for a single owner.
//
// sync.Pool is a cache rather than a pool: the runtime clears it during
// garbage collection, keeping one generation in a victim cache. For
// most work that is the right trade, since idle memory returns to the
// heap. On a latency-critical path it is backwards - every few seconds
// the pool empties and the allocations it exists to avoid all happen
// again, in a burst, on whichever goroutine is unlucky. That is the
// opposite of what a pool is for when the goal is a predictable tail
// rather than a good average.
//
// Aeron's Java side pre-allocates and then does not allocate at all in
// steady state, and its pools are per-thread arrays with no
// synchronisation - there is nothing to synchronise, so they beat any
// shared structure including a lock-free one. A Pool is that: an
// ordinary slice, traced by the collector like any other live
// reference, so nothing is reclaimed while the pool exists.
//
// Not safe for concurrent use, deliberately. Give each goroutine its
// own, or guard it externally. Sharing one without a lock will corrupt
// it, and the race detector will say so.
//
// Values are reused last-in-first-out, since the most recently released
// value is the most likely to still be in cache.
type Pool[T any] struct {
	free  []T
	new   func() T
	reset func(T)

	// misses counts Gets that found the pool empty and had to
	// construct. An undersized pool is otherwise invisible: it keeps
	// working and quietly allocates, which is the thing it was added to
	// stop.
	misses uint64
}

// NewPool returns a Pool holding at most capacity values, pre-filled so
// the steady state never constructs.
//
// newFn builds a value when the pool is empty. resetFn, if non-nil, is
// applied on Put so a value returns clean; pass nil when the caller
// already resets, as a buffer re-sliced to zero length does.
//
// A capacity below one is raised to one: a pool that can hold nothing
// constructs on every Get, which is a silent cliff rather than an error
// worth returning.
func NewPool[T any](capacity int, newFn func() T, resetFn func(T)) *Pool[T] {
	if capacity < 1 {
		capacity = 1
	}
	p := &Pool[T]{
		free:  make([]T, 0, capacity),
		new:   newFn,
		reset: resetFn,
	}
	for i := 0; i < capacity; i++ {
		p.free = append(p.free, newFn())
	}
	return p
}

// Get takes a value from the pool, constructing one if it is empty.
func (p *Pool[T]) Get() T {
	if n := len(p.free); n > 0 {
		v := p.free[n-1]
		var zero T
		p.free[n-1] = zero // drop the pool's reference, so a value the caller keeps is not pinned here
		p.free = p.free[:n-1]
		return v
	}
	p.misses++
	return p.new()
}

// Put returns a value to the pool, discarding it if the pool is full.
//
// The caller must not retain v afterwards: the next Get may hand it out
// again.
func (p *Pool[T]) Put(v T) {
	if p.reset != nil {
		p.reset(v)
	}
	if len(p.free) == cap(p.free) {
		return
	}
	p.free = append(p.free, v)
}

// Misses reports how many times Get found the pool empty. A figure that
// keeps climbing under steady load means the capacity is too small.
func (p *Pool[T]) Misses() uint64 { return p.misses }

// Len reports how many values are currently available.
func (p *Pool[T]) Len() int { return len(p.free) }

// Cap reports the pool's fixed capacity.
func (p *Pool[T]) Cap() int { return cap(p.free) }
