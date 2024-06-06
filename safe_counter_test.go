package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeCounter(t *testing.T) {
	counter := &SafeCounter{}

	// Test initial value
	initialValue := counter.Value()
	assert.Equal(
		t,
		0,
		initialValue,
		"initial counter value should be 0",
	)

	// Test single increment
	counter.Increment(5)
	valueAfterIncrement := counter.Value()
	assert.Equal(
		t,
		5,
		valueAfterIncrement,
		"counter value should be 5 after incrementing by 5",
	)

	// Test multiple increments
	counter.Increment(3)
	counter.Increment(2)
	valueAfterMultipleIncrements := counter.Value()
	assert.Equal(
		t,
		10,
		valueAfterMultipleIncrements,
		"counter value should be 10 after incrementing by 5, 3, and 2",
	)

	// Test concurrent increments
	numGoroutines := 1000
	incrementValue := 1

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter.Increment(incrementValue)
		}()
	}
	wg.Wait()

	finalValue := counter.Value()
	expectedFinalValue := 10 + numGoroutines*incrementValue
	require.Equal(
		t,
		expectedFinalValue,
		finalValue,
		"counter value should be correct after concurrent increments",
	)
}
