package batcher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var (
	expected = [][]byte{
		[]byte("hello"),
		[]byte("amazing"),
		[]byte("world"),
	}
)

func TestInvalidConfig(t *testing.T) {
	validConfig := scoop_protocol.BatcherConfig{
		MaxSize:      1 * 1024 * 1024,
		MaxEntries:   -1,
		MaxAge:       "1m",
		BufferLength: 5,
	}
	assert.NoError(t, validConfig.Validate())

	config := validConfig
	config.MaxSize = 0
	assert.Error(t, config.Validate())

	config = validConfig
	config.MaxAge = "foo"
	assert.Error(t, config.Validate())

	config = validConfig
	config.MaxAge = "0"
	assert.Error(t, config.Validate())

	config = validConfig
	config.BufferLength = 0
	assert.Error(t, config.Validate())
}

func TestBatcher(t *testing.T) {
	config := scoop_protocol.BatcherConfig{
		MaxSize:      1 * 1024 * 1024,
		MaxEntries:   -1,
		MaxAge:       "1m",
		BufferLength: 5,
	}

	var result [][]byte

	b, err := New(config, func(batch [][]byte) {
		result = batch
	})

	assert.NoError(t, err)

	for _, e := range expected {
		b.Submit(e)
	}
	b.Close()

	assert.EqualValues(t, expected, result)
}

func TestTimeout(t *testing.T) {
	config := scoop_protocol.BatcherConfig{
		MaxSize:      1 * 1024 * 1024,
		MaxEntries:   -1,
		MaxAge:       "500ms",
		BufferLength: 5,
	}

	var result [][]byte

	b, err := New(config, func(batch [][]byte) {
		result = batch
	})

	assert.NoError(t, err)

	for _, e := range expected {
		b.Submit(e)
	}
	time.Sleep(1 * time.Second)
	assert.True(t, len(result) > 0)
	b.Close()
}

func TestSizeLimit(t *testing.T) {
	config := scoop_protocol.BatcherConfig{
		MaxSize:      10,
		MaxEntries:   -1,
		MaxAge:       "1m",
		BufferLength: 5,
	}

	var result [][]byte

	b, err := New(config, func(batch [][]byte) {
		result = batch
	})

	assert.NoError(t, err)

	for _, e := range expected {
		b.Submit(e)
	}
	time.Sleep(1 * time.Second)
	assert.True(t, len(result) > 0)
	b.Close()
}

func TestMaxEntryLimit(t *testing.T) {
	config := scoop_protocol.BatcherConfig{
		MaxSize:      1 * 1024 * 1024,
		MaxEntries:   2,
		MaxAge:       "1m",
		BufferLength: 5,
	}

	var result [][]byte

	b, err := New(config, func(batch [][]byte) {
		result = batch
	})

	assert.NoError(t, err)

	for _, e := range expected {
		b.Submit(e)
	}
	time.Sleep(1 * time.Second)
	assert.True(t, len(result) <= 2)
	b.Close()
}
