package timedmap

import (
	"sync"
	"time"

	"github.com/zekroTJA/timedmap/maybe"
)

type callback[V any] func(value V)

// TimedMap contains a map with all key-value pairs,
// and a timer, which cleans the map in the set
// tick durations from expired keys.
type TimedMap[K comparable, V any] struct {
	mtx             sync.RWMutex
	container       map[keyWrap[K]]element[V]
	elementPool     sync.Pool
	cleanupTickTime time.Duration
	cleanerTicker   *time.Ticker
	cleanerStopChan chan bool
	cleanerRunning  bool
}

type keyWrap[T comparable] struct {
	sec int
	key T
}

// element contains the actual value as interface type,
// the time when the value expires and an array of
// callbacks, which will be executed when the element
// expires.
type element[V any] struct {
	value   V
	expires time.Time
	cbs     []callback[V]
}

// New creates and returns a new instance of TimedMap.
// The passed cleanupTickTime will be passed to the
// cleanup ticker, which iterates through the map and
// deletes expired key-value pairs.
//
// Optionally, you can also pass a custom <-chan time.Time
// which controls the cleanup cycle if you want to use
// a single synchronized timer or if you want to have more
// control over the cleanup loop.
//
// When passing 0 as cleanupTickTime and no tickerChan,
// the cleanup loop will not be started. You can call
// StartCleanerInternal or StartCleanerExternal to
// manually start the cleanup loop. These both methods
// can also be used to re-define the specification of
// the cleanup loop when already running if you want to.
func New[K comparable, V any](cleanupTickTime time.Duration, tickerChan ...<-chan time.Time) *TimedMap[K, V] {
	tm := &TimedMap[K, V]{
		container:       make(map[keyWrap[K]]element[V]),
		cleanerStopChan: make(chan bool),
		elementPool: sync.Pool{
			New: func() interface{} {
				return element[V]{}
			},
		},
	}

	if len(tickerChan) > 0 {
		tm.StartCleanerExternal(tickerChan[0])
	} else if cleanupTickTime > 0 {
		tm.StartCleanerInternal(cleanupTickTime)
	}

	return tm
}

// Ident returns the current sections ident.
// In the case of the root object TimedMap,
// this is always 0.
func (tm *TimedMap[K, V]) Ident() int {
	return 0
}

// Set appends a key-value pair to the map or sets the value of
// a key. expiresAfter sets the expiry time after the key-value pair
// will automatically be removed from the map.
func (tm *TimedMap[K, V]) Set(key K, value V, expiresAfter time.Duration, cb ...callback[V]) {
	tm.set(key, 0, value, expiresAfter, cb...)
}

// GetValue returns an interface of the value of a key in the
// map. The returned value is nil if there is no value to the
// passed key or if the value was expired.
func (tm *TimedMap[K, V]) GetValue(key K) maybe.Maybe[V] {
	v := tm.get(key, 0)
	if v.IsEmpty() {
		return maybe.None[V]()
	}
	return maybe.From(v.Unfold().value)
}

// GetExpires returns the expire time of a key-value pair.
// If the key-value pair does not exist in the map or
// was expired, this will return an error object.
func (tm *TimedMap[K, V]) GetExpires(key K) (time.Time, error) {
	v := tm.get(key, 0)
	if v.IsEmpty() {
		return time.Time{}, ErrKeyNotFound
	}
	return v.Unfold().expires, nil
}

// SetExpire is deprecated.
// Please use SetExpires instead.
func (tm *TimedMap[K, V]) SetExpire(key K, d time.Duration) error {
	return tm.SetExpires(key, d)
}

// SetExpires sets the expire time for a key-value
// pair to the passed duration. If there is no value
// to the key passed , this will return an error.
func (tm *TimedMap[K, V]) SetExpires(key K, d time.Duration) error {
	return tm.setExpires(key, 0, d)
}

// Contains returns true, if the key exists in the map.
// false will be returned, if there is no value to the
// key or if the key-value pair was expired.
func (tm *TimedMap[K, V]) Contains(key K) bool {
	return tm.get(key, 0).IsAny()
}

// Remove deletes a key-value pair in the map.
func (tm *TimedMap[K, V]) Remove(key K) {
	tm.remove(key, 0)
}

// Refresh extends the expire time for a key-value pair
// about the passed duration. If there is no value to
// the key passed, this will return an error object.
func (tm *TimedMap[K, V]) Refresh(key K, d time.Duration) error {
	return tm.refresh(key, 0, d)
}

// Flush deletes all key-value pairs of the map.
func (tm *TimedMap[K, V]) Flush() {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	for k, v := range tm.container {
		tm.elementPool.Put(v)
		delete(tm.container, k)
	}
}

// Size returns the current number of key-value pairs
// existent in the map.
func (tm *TimedMap[K, V]) Size() int {
	return len(tm.container)
}

// StartCleanerInternal starts the cleanup loop controlled
// by an internal ticker with the given interval.
//
// If the cleanup loop is already running, it will be
// stopped and restarted using the new specification.
func (tm *TimedMap[K, V]) StartCleanerInternal(interval time.Duration) {
	if tm.cleanerRunning {
		tm.StopCleaner()
	}
	tm.cleanerTicker = time.NewTicker(interval)
	go tm.cleanupLoop(tm.cleanerTicker.C)
}

// StartCleanerExternal starts the cleanup loop controlled
// by the given initiator channel. This is useful if you
// want to have more control over the cleanup loop or if
// you want to sync up multiple timedmaps.
//
// If the cleanup loop is already running, it will be
// stopped and restarted using the new specification.
func (tm *TimedMap[K, V]) StartCleanerExternal(initiator <-chan time.Time) {
	if tm.cleanerRunning {
		tm.StopCleaner()
	}
	go tm.cleanupLoop(initiator)
}

// StopCleaner stops the cleaner go routine and timer.
// This should always be called after exiting a scope
// where TimedMap is used that the data can be cleaned
// up correctly.
func (tm *TimedMap[K, V]) StopCleaner() {
	if !tm.cleanerRunning {
		return
	}
	tm.cleanerStopChan <- true
	if tm.cleanerTicker != nil {
		tm.cleanerTicker.Stop()
	}
}

// Snapshot returns a new map which represents the
// current key-value state of the internal container.
func (tm *TimedMap[K, V]) Snapshot() map[K]V {
	return tm.getSnapshot(0)
}

// cleanupLoop holds the loop executing the cleanup
// when initiated by tc.
func (tm *TimedMap[K, V]) cleanupLoop(tc <-chan time.Time) {
	tm.cleanerRunning = true
	defer func() {
		tm.cleanerRunning = false
	}()

	for {
		select {
		case <-tc:
			tm.cleanUp()
		case <-tm.cleanerStopChan:
			return
		}
	}
}

// expireElement removes the specified key-value element
// from the map and executes all defined callback functions
func (tm *TimedMap[K, V]) expireElement(key K, sec int, v element[V]) {
	for _, cb := range v.cbs {
		cb(v.value)
	}

	k := keyWrap[K]{
		sec: sec,
		key: key,
	}

	tm.elementPool.Put(v)
	delete(tm.container, k)
}

// cleanUp iterates trhough the map and expires all key-value
// pairs which expire time after the current time
func (tm *TimedMap[K, V]) cleanUp() {
	now := time.Now()

	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	for k, v := range tm.container {
		if now.After(v.expires) {
			tm.expireElement(k.key, k.sec, v)
		}
	}
}

// set sets the value for a key and section with the
// given expiration parameters
func (tm *TimedMap[K, V]) set(key K, sec int, val V, expiresAfter time.Duration, cb ...callback[V]) {
	// re-use element when existent on this key
	if v := tm.getRaw(key, sec); v.IsAny() {
		inside := v.Unfold()
		inside.value = val
		inside.expires = time.Now().Add(expiresAfter)
		inside.cbs = cb
		return
	}

	k := keyWrap[K]{
		sec: sec,
		key: key,
	}

	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	v := tm.elementPool.Get().(element[V])
	v.value = val
	v.expires = time.Now().Add(expiresAfter)
	v.cbs = cb
	tm.container[k] = v
}

// get returns an element object by key and section
// if the value has not already expired
func (tm *TimedMap[K, V]) get(key K, sec int) maybe.Maybe[element[V]] {
	v := tm.getRaw(key, sec)

	if v.IsEmpty() {
		return maybe.None[element[V]]()
	}

	inside := v.Unfold()
	if time.Now().After(inside.expires) {
		tm.mtx.Lock()
		defer tm.mtx.Unlock()
		tm.expireElement(key, sec, inside)
		return maybe.None[element[V]]()
	}

	return maybe.From[element[V]](inside)
}

// getRaw returns the raw element object by key,
// not depending on expiration time
func (tm *TimedMap[K, V]) getRaw(key K, sec int) maybe.Maybe[element[V]] {
	k := keyWrap[K]{
		sec: sec,
		key: key,
	}

	tm.mtx.RLock()
	v, ok := tm.container[k]
	tm.mtx.RUnlock()

	if !ok {
		return maybe.None[element[V]]()
	}

	return maybe.From[element[V]](v)
}

// remove removes an element from the map by given
// key and section
func (tm *TimedMap[K, V]) remove(key K, sec int) {
	k := keyWrap[K]{
		sec: sec,
		key: key,
	}

	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	v, ok := tm.container[k]
	if !ok {
		return
	}

	tm.elementPool.Put(v)
	delete(tm.container, k)
}

// refresh extends the lifetime of the given key in the
// given section by the duration d.
func (tm *TimedMap[K, V]) refresh(key K, sec int, d time.Duration) error {
	v := tm.get(key, sec)
	if v.IsEmpty() {
		return ErrKeyNotFound
	}
	inside := v.Unfold()
	inside.expires = inside.expires.Add(d)
	return nil
}

// setExpires sets the lifetime of the given key in the
// given section to the duration d.
func (tm *TimedMap[K, V]) setExpires(key K, sec int, d time.Duration) error {
	v := tm.get(key, sec)
	if v.IsEmpty() {
		return ErrKeyNotFound
	}
	inside := v.Unfold()
	inside.expires = time.Now().Add(d)
	return nil
}

func (tm *TimedMap[K, V]) getSnapshot(sec int) (m map[K]V) {
	m = make(map[K]V)

	tm.mtx.RLock()
	defer tm.mtx.RUnlock()

	for k, v := range tm.container {
		if k.sec == sec {
			m[k.key] = v.value
		}
	}

	return
}
