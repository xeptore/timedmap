package main

import (
	"log"
	"time"

	"github.com/xeptore/timedmap/v3"
)

func main() {

	// Creates a new timed map which scans for
	// expired keys every 1 second
	tm := timedmap.New[string, int](1 * time.Second)

	// Add a key "hey" with the value 213, which should
	// expire after 3 seconds and execute the callback, which
	// prints that the key was expired
	tm.Set("hey", 213, 3*time.Second)

	// Print key "hey" from timed map
	printKeyVal(tm, "hey")

	// Wait for 5 seconds
	// During this time the main thread is blocked, the
	// key-value pair of "hey" will be expired
	time.Sleep(5 * time.Second)

	// Printing value of key "hey" wil lfail because the
	// key-value pair does not exist anymore
	printKeyVal(tm, "hey")
}

func printKeyVal[K comparable, V any](tm *timedmap.TimedMap[K, V], key K) {
	d := tm.GetValue(key)
	if d == nil {
		log.Println("data expired")
		return
	}

	log.Printf("%v = %+v\n", key, d)
}
