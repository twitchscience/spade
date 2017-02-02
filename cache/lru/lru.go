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
	"time"
)

// LongDuration is one year; use when you don't really want
// a timeout on the cache (e.g. in tests).
const LongDuration = time.Duration(365 * 24 * time.Hour)

// ErrCacheMiss is an error indicating that the value is not in the cache.
var ErrCacheMiss = errors.New("lru cache miss")

// Cache is an LRU cache, safe for concurrent access.
type Cache struct {
	maxEntries  int
	lifetime    time.Duration
	currentTime func() time.Time

	mu    sync.Mutex
	ll    *list.List
	cache map[string]*list.Element
}

// *entry is the type stored in each *list.Element.
type entry struct {
	key, value string
	expiration time.Time
}

// New returns a new cache with the provided maximum items.
func New(maxEntries int, lifetime time.Duration) *Cache {
	return newWithTimeFunction(maxEntries, lifetime, time.Now)
}

func newWithTimeFunction(maxEntries int, lifetime time.Duration, currentTime func() time.Time) *Cache {
	return &Cache{
		maxEntries:  maxEntries,
		lifetime:    lifetime,
		currentTime: currentTime,
		ll:          list.New(),
		cache:       make(map[string]*list.Element),
	}
}

// Set adds the provided key and value to the cache, evicting
// an old item if necessary.
func (c *Cache) Set(key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already present, move to front of queue and restart the clock
	// (no need to update map as we're modifying the entry it already contains)
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ent := ee.Value.(*entry)
		ent.value = value
		ent.expiration = c.newExpiration()
		return nil
	}

	// If not present, add to cache and queue
	ele := c.ll.PushFront(&entry{key, value, c.newExpiration()})
	c.cache[key] = ele

	if c.ll.Len() > c.maxEntries {
		c.removeOldest()
	}
	return nil
}

// Get fetches the key's value from the cache.
// The error result will be nil if the item was found.
// Note that while the entry will be moved to the front of the queue, its expiration
// clock will not restart, and it will be removed if it has expired.
func (c *Cache) Get(key string) (value string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ele, hit := c.cache[key]
	if !hit {
		return "", ErrCacheMiss
	}

	ent := ele.Value.(*entry)
	if ent.expiration.Before(c.currentTime()) {
		delete(c.cache, key)
		c.ll.Remove(ele)
		return "", ErrCacheMiss
	}

	c.ll.MoveToFront(ele)
	return ent.value, nil
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

func (c *Cache) newExpiration() time.Time {
	return c.currentTime().Add(c.lifetime)
}
