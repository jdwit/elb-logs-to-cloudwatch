package main

import (
	"sync"
)

type SafeCounter struct {
	mu sync.Mutex
	v  int
}

func (c *SafeCounter) Increment(value int) {
	c.mu.Lock()
	c.v += value
	c.mu.Unlock()
}

func (c *SafeCounter) Value() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}
