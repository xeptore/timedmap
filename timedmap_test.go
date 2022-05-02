package timedmap

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	dCleanupTick = 10 * time.Millisecond
)

func TestNew(t *testing.T) {
	tm := New[string, int](dCleanupTick)

	assert.NotNil(t, tm)
	assert.EqualValues(t, 0, len(tm.container))
	time.Sleep(10 * time.Millisecond)
	assert.True(t, tm.cleanerRunning)
}

func TestFlush(t *testing.T) {
	tm := New[int, int](dCleanupTick)

	for i := 0; i < 10; i++ {
		tm.set(i, 1, time.Hour)
	}
	assert.EqualValues(t, 10, len(tm.container))
	tm.Flush()
	assert.EqualValues(t, 0, len(tm.container))
}

func TestIdent(t *testing.T) {
	tm := New[int, int](dCleanupTick)
	assert.EqualValues(t, 0, tm.Ident())
}

func TestSet(t *testing.T) {
	const key = "tKeySet"
	const val = "tValSet"

	tm := New[string, string](dCleanupTick)

	tm.Set(key, val, 20*time.Millisecond)
	if v := tm.get(key); v.IsEmpty() {
		t.Fatal("key was not set")
	}
	assert.Equal(t, val, tm.get(key).Unfold().value)

	time.Sleep(40 * time.Millisecond)
	assert.Panics(t, func() {
		tm.get(key).Unfold()
	})
}

func TestGetValue(t *testing.T) {
	const key = "tKeyGetVal"
	const val = "tValGetVal"

	tm := New[string, string](dCleanupTick)

	tm.Set(key, val, 50*time.Millisecond)
	assert.True(t, tm.GetValue("keyNotExists").IsEmpty())

	assert.Equal(t, val, tm.GetValue(key).Unfold())

	time.Sleep(60 * time.Millisecond)

	assert.Panics(t, func() {
		tm.GetValue(key).Unfold()
	})

	tm.Set(key, val, 1*time.Microsecond)
	time.Sleep(2 * time.Millisecond)
	assert.Panics(t, func() {
		tm.GetValue(key).Unfold()
	})
}

func TestGetExpire(t *testing.T) {
	const key = "tKeyGetExp"
	const val = "tValGetExp"

	tm := New[string, string](dCleanupTick)

	tm.Set(key, val, 50*time.Millisecond)
	ct := time.Now().Add(50 * time.Millisecond)

	_, err := tm.GetExpires("keyNotExists")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	exp, err := tm.GetExpires(key)
	assert.Nil(t, err)
	assert.Less(t, ct.Sub(exp), 1*time.Millisecond)
}

func TestSetExpires(t *testing.T) {
	const key = "tKeyRef"

	tm := New[string, int](dCleanupTick)

	err := tm.Refresh("keyNotExists", time.Hour)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = tm.SetExpires("notExistentKey", 1*time.Second)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	tm.Set(key, 1, 12*time.Millisecond)
	err = tm.SetExpires(key, 50*time.Millisecond)
	assert.Nil(t, err)

	fmt.Println(time.Now().UnixMilli())
	time.Sleep(12 * time.Millisecond)
	assert.NotPanics(t, func() {
		fmt.Println(time.Now().UnixMilli())
		tm.get(key).Unfold()
	})

	time.Sleep(52 * time.Millisecond)
	assert.Panics(t, func() {
		tm.get(key).Unfold()
	})
}

func TestContains(t *testing.T) {
	const key = "tKeyCont"

	tm := New[string, int](dCleanupTick)

	tm.Set(key, 1, 30*time.Millisecond)

	assert.False(t, tm.Contains("keyNotExists"))
	assert.True(t, tm.Contains(key))

	time.Sleep(50 * time.Millisecond)
	assert.False(t, tm.Contains(key))
}

func TestRemove(t *testing.T) {
	const key = "tKeyRem"

	tm := New[string, int](dCleanupTick)

	tm.Set(key, 1, time.Hour)
	tm.Remove(key)

	assert.Panics(t, func() {
		tm.get(key).Unfold()
	})
}

func TestRefresh(t *testing.T) {
	const key = "tKeyRef"

	tm := New[string, int](dCleanupTick)

	err := tm.Refresh("keyNotExists", time.Hour)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	tm.Set(key, 1, 12*time.Millisecond)
	assert.Nil(t, tm.Refresh(key, 50*time.Millisecond))

	time.Sleep(30 * time.Millisecond)
	assert.NotPanics(t, func() {
		tm.get(key).Unfold()
	})

	time.Sleep(100 * time.Millisecond)
	assert.True(t, tm.get(key).IsEmpty())
}

func TestSize(t *testing.T) {
	tm := New[int, int](dCleanupTick)

	for i := 0; i < 25; i++ {
		tm.Set(i, 1, 50*time.Millisecond)
	}
	assert.EqualValues(t, 25, tm.Size())
}

func TestStopCleaner(t *testing.T) {
	tm := New[int, int](dCleanupTick)

	time.Sleep(10 * time.Millisecond)
	tm.StopCleaner()
	time.Sleep(10 * time.Millisecond)
	assert.False(t, tm.cleanerRunning)

	assert.NotPanics(t, func() {
		tm.StopCleaner()
	})
}

func TestStartCleanerInternal(t *testing.T) {
	// Test functionality
	{
		tm := New[int, int](0)
		time.Sleep(10 * time.Millisecond)

		assert.False(t, tm.cleanerRunning)

		// Ensure cleanup timer is not running
		tm.set(1, 1, 0)
		time.Sleep(100 * time.Millisecond)
		assert.EqualValues(t, 1, tm.getRaw(1).Unfold().value)

		tm.StartCleanerInternal(dCleanupTick)
		time.Sleep(10 * time.Millisecond)
		assert.True(t, tm.cleanerRunning)

		// Ensure cleanup timer is running
		tm.set(1, 1, 0)
		time.Sleep(100 * time.Millisecond)
		assert.Panics(t, func() {
			tm.getRaw(1).Unfold()
		})
	}

	// Test ticker overwrite and cleaner stop
	{
		tm := New[int, int](dCleanupTick)
		time.Sleep(10 * time.Millisecond)

		oldTicker := tm.cleanerTicker

		tm.StartCleanerInternal(2 * dCleanupTick)
		assert.NotEqual(t, oldTicker, tm.cleanerTicker)
	}
}

func TestStartCleanerExternal(t *testing.T) {
	// Test functionality
	{
		tm := New[int, int](0)
		time.Sleep(10 * time.Millisecond)

		assert.False(t, tm.cleanerRunning)

		// Ensure cleanup timer is not running
		tm.set(1, 1, 0)
		time.Sleep(100 * time.Millisecond)
		assert.EqualValues(t, 1, tm.getRaw(1).Unfold().value)

		c := make(chan time.Time)

		tm.StartCleanerExternal(c)
		time.Sleep(10 * time.Millisecond)
		assert.True(t, tm.cleanerRunning)

		// Ensure cleanup is controlled by c
		tm.set(1, 1, 0)
		time.Sleep(100 * time.Millisecond)
		assert.NotPanics(t, func() {
			tm.getRaw(1).Unfold()
		})

		// Ensure cleanup is controlled by c
		c <- time.Now()
		time.Sleep(10 * time.Millisecond)
		assert.Panics(t, func() {
			tm.getRaw(1).Unfold()
		})
	}

	// Ensure timer overwrite
	{
		tm := New[int, int](dCleanupTick)
		time.Sleep(10 * time.Millisecond)

		assert.True(t, tm.cleanerRunning)
		assert.NotNil(t, tm.cleanerTicker)

		c := make(chan time.Time)
		tm.StartCleanerExternal(c)

		// Ensure cleanup is controlled by c
		tm.set(1, 1, 0)
		time.Sleep(100 * time.Millisecond)
		assert.NotPanics(t, func() {
			tm.getRaw(1).Unfold()
		})
	}
}

func TestSnapshot(t *testing.T) {
	tm := New[int, int](1 * time.Minute)

	for i := 0; i < 10; i++ {
		tm.set(i, i, 1*time.Minute)
	}

	m := tm.Snapshot()

	assert.Len(t, m, 10)
	for i := 0; i < 10; i++ {
		assert.EqualValues(t, i, m[i])
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	tm := New[int, int](dCleanupTick)

	go func() {
		for {
			for i := 0; i < 10; i++ {
				tm.Set(i, i, 2*time.Second)
			}
		}
	}()

	// Wait 10 mills before read cycle starts so that
	// it does not start before the first values are
	// set to the map.
	time.Sleep(10 * time.Millisecond)
	go func() {
		for {
			for i := 0; i < 10; i++ {
				v := tm.GetValue(i)
				assert.EqualValues(t, i, v.Unfold())
			}
		}
	}()

	time.Sleep(1 * time.Second)
}

func TestGetExpiredConcurrent(t *testing.T) {
	tm := New[int, int](dCleanupTick)

	wg := sync.WaitGroup{}
	for i := 0; i < 50000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tm.Set(1, 1, 0)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			tm.GetValue(1)
		}()
	}

	wg.Wait()
}

func TestExternalTicker(t *testing.T) {
	const key = "tKeySet"
	const val = "tValSet"

	ticker := time.NewTicker(dCleanupTick)
	tm := New[string, string](0, ticker.C)

	tm.Set(key, val, 20*time.Millisecond)
	assert.Equal(t, val, tm.get(key).Unfold().value)

	time.Sleep(40 * time.Millisecond)
	assert.Panics(t, func() {
		tm.get(key).Unfold()
	})
}

func TestBeforeCleanup(t *testing.T) {
	const key, value = 1, 2

	tm := New[int, int](1 * time.Hour)

	tm.Set(key, value, 5*time.Millisecond)

	time.Sleep(10 * time.Millisecond)
}

// ----------------------------------------------------------
// --- BENCHMARKS ---

func BenchmarkSetValues(b *testing.B) {
	tm := New[int, int](1 * time.Minute)
	for n := 0; n < b.N; n++ {
		tm.Set(n, n, 1*time.Hour)
	}
}

func BenchmarkSetGetValues(b *testing.B) {
	tm := New[int, int](1 * time.Minute)
	for n := 0; n < b.N; n++ {
		tm.Set(n, n, 1*time.Hour)
		tm.GetValue(n)
	}
}

func BenchmarkSetGetRemoveValues(b *testing.B) {
	tm := New[int, int](1 * time.Minute)
	for n := 0; n < b.N; n++ {
		tm.Set(n, n, 1*time.Hour)
		tm.GetValue(n)
		tm.Remove(n)
	}
}

func BenchmarkSetGetSameKey(b *testing.B) {
	tm := New[int, int](1 * time.Minute)
	for n := 0; n < b.N; n++ {
		tm.Set(1, n, 1*time.Hour)
		tm.GetValue(1)
	}
}
