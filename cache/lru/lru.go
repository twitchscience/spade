// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package lru implements an LRU cache.
// Adapted from golang.org/x/build/internal/lru.
package lru

import (
	"container/list"
	"errors"
	"sync"
)

// ErrCacheMiss is an error indicating that the value is not in the cache.
var ErrCacheMiss = errors.New("lru cache miss")

// Cache is an LRU cache, safe for concurrent access.
type Cache struct {
	maxEntries int

	mu    sync.Mutex
	ll    *list.List
	cache map[string]*list.Element
}

// *entry is the type stored in each *list.Element.
type entry struct {
	key, value string
}

// New returns a new cache with the provided maximum items.
func New(maxEntries int) *Cache {
	return &Cache{
		maxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
	}
}

// Set adds the provided key and value to the cache, evicting
// an old item if necessary.
func (c *Cache) Set(key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Already in cache?
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return nil
	}

	// Add to cache if not present
	ele := c.ll.PushFront(&entry{key, value})
	c.cache[key] = ele

	if c.ll.Len() > c.maxEntries {
		c.removeOldest()
	}
	return nil
}

// Get fetches the key's value from the cache.
// The error result will be nil if the item was found.
func (c *Cache) Get(key string) (value string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, nil
	}
	return "", ErrCacheMiss
}

// RemoveOldest removes the oldest item in the cache and returns its key and value.
// If the cache is empty, two empty strings are returned.
func (c *Cache) RemoveOldest() (key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeOldest()
}

// note: must hold c.mu
func (c *Cache) removeOldest() (key, value string) {
	ele := c.ll.Back()
	if ele == nil {
		return
	}
	c.ll.Remove(ele)
	ent := ele.Value.(*entry)
	delete(c.cache, ent.key)
	return ent.key, ent.value

}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
