// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lru

import (
	"reflect"
	"testing"
	"time"
)

func expectHit(t *testing.T, c *Cache, k, ev string) {
	v, err := c.Get(k)
	if err != nil {
		t.Fatalf("expected cache(%q)=%v; but missed", k, ev)
	}
	if !reflect.DeepEqual(v, ev) {
		t.Fatalf("expected cache(%q)=%v; but got %v", k, ev, v)
	}
}

func expectMiss(t *testing.T, c *Cache, k string) {
	if v, err := c.Get(k); err == nil {
		t.Fatalf("expected cache miss on key %q but hit value %v", k, v)
	} else if err != ErrCacheMiss {
		t.Fatalf("Unexpected error %v on key %q, should be %q", err, k, ErrCacheMiss)
	}
}

func TestLRU(t *testing.T) {
	c := New(2, LongDuration)

	expectMiss := func(k string) { expectMiss(t, c, k) }
	expectHit := func(k string, ev string) { expectHit(t, c, k, ev) }

	// cache is empty
	expectMiss("1")

	_ = c.Set("1", "one") // (1, one) -> nil
	expectHit("1", "one")

	_ = c.Set("2", "two") // (2, two) -> (1, one) -> nil
	expectHit("1", "one")
	expectHit("2", "two")

	_ = c.Set("3", "three") // (3, three) -> (2, two) -> nil
	expectHit("3", "three")
	expectHit("2", "two") // (2, two) -> (3, three) -> nil
	expectMiss("1")

	_ = c.Set("2", "dos")   // (2, dos) -> (3, three) -> nil
	expectHit("2", "dos")   // (2, dos) -> (3, three) -> nil
	expectHit("3", "three") // (3, three) -> (2, dos) -> nil
	expectMiss("1")

	_ = c.Set("1", "uno")   // (1, uno) -> (3, three) -> nil
	expectHit("3", "three") // (3, three) -> (1, uno) -> nil
	expectMiss("2")
	expectHit("1", "uno") // (1, uno) -> (3, three) -> nil

	if k, v := c.RemoveOldest(); k != "3" || v != "three" {
		t.Fatalf("oldest = %q, %q, expected \"3\", \"three\"", k, v)
	}
	expectHit("1", "uno")
	expectMiss("2")
	expectMiss("3")
}

func TestRemoveOldest(t *testing.T) {
	c := New(2, LongDuration)
	_ = c.Set("1", "one")
	_ = c.Set("2", "two")
	if k, v := c.RemoveOldest(); k != "1" || v != "one" {
		t.Fatalf("oldest = %q, %q; want 1, one", k, v)
	}
	if k, v := c.RemoveOldest(); k != "2" || v != "two" {
		t.Fatalf("oldest = %q, %q; want 2, two", k, v)
	}
	if k, v := c.RemoveOldest(); k != "" || v != "" {
		t.Fatalf("oldest = %v, %v; want \"\", \"\"", k, v)
	}
}

func TestZeroTimeout(t *testing.T) {
	currentTime := time.Now()
	c := newWithTimeFunction(2, time.Duration(0), func() time.Time { return currentTime })
	expectMiss := func(k string) { expectMiss(t, c, k) }

	_ = c.Set("1", "one")
	currentTime = currentTime.Add(time.Millisecond)
	expectMiss("1")

	_ = c.Set("1", "one")
	_ = c.Set("2", "two")
	currentTime = currentTime.Add(time.Millisecond)
	expectMiss("1")

	_ = c.Set("1", "one")
	if size := c.Len(); size != 2 {
		t.Error("cache should have 2 elements (both expired)")
	}
	if k, v := c.RemoveOldest(); k != "2" || v != "two" {
		t.Errorf("oldest = %q, %q; expected \"2\", \"two\"", k, v)
	}
}

func TestShortTimeout(t *testing.T) {
	currentTime := time.Now()
	c := newWithTimeFunction(2, 100*time.Millisecond, func() time.Time { return currentTime })

	expectMiss := func(k string) { expectMiss(t, c, k) }
	expectHit := func(k string, ev string) { expectHit(t, c, k, ev) }

	_ = c.Set("1", "one")
	currentTime = currentTime.Add(60 * time.Millisecond)
	_ = c.Set("2", "two")
	currentTime = currentTime.Add(60 * time.Millisecond)

	expectHit("2", "two")
	expectMiss("1")
}
