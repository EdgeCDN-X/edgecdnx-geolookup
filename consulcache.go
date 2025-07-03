package edgecdnxgeolookup

import (
	"sync"
	"time"
)

type cacheItem[T any] struct {
	value     T
	expiresAt time.Time
}

type Cache[T any] struct {
	data      map[string]cacheItem[T]
	ttl       time.Duration
	mutex     sync.RWMutex
	stopChan  chan struct{}
	cleanupWG sync.WaitGroup
}

// NewCache creates a new generic cache with default TTL and cleanup interval.
func NewCache[T any](ttl time.Duration, cleanupInterval time.Duration) *Cache[T] {
	c := &Cache[T]{
		data:     make(map[string]cacheItem[T]),
		ttl:      ttl,
		stopChan: make(chan struct{}),
	}

	c.cleanupWG.Add(1)
	go c.cleanupExpired(cleanupInterval)

	return c
}

// Set stores a value for a given key with the default TTL.
func (c *Cache[T]) Set(key string, value T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data[key] = cacheItem[T]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Get returns the value and whether it was found and not expired.
func (c *Cache[T]) Get(key string) (T, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	item, ok := c.data[key]
	if !ok || time.Now().After(item.expiresAt) {
		var zero T
		return zero, false
	}
	return item.value, true
}

// Delete removes a key.
func (c *Cache[T]) Delete(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.data, key)
}

// cleanupExpired removes expired entries on a timer.
func (c *Cache[T]) cleanupExpired(interval time.Duration) {
	defer c.cleanupWG.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.mutex.Lock()
			for k, v := range c.data {
				if now.After(v.expiresAt) {
					delete(c.data, k)
				}
			}
			c.mutex.Unlock()
		case <-c.stopChan:
			return
		}
	}
}

// Stop shuts down the background cleanup process.
func (c *Cache[T]) Stop() {
	close(c.stopChan)
	c.cleanupWG.Wait()
}
