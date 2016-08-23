package deglobber

import (
	"bytes"
	"compress/flate"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	cache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

var (
	testMsg1 = []byte(`[{"uuid": "x", "data": "test"}]`)
	testMsg2 = []byte(`[{"uuid": "y", "data": "test3"}]`)
	dupeMsg  = []byte(`[{"uuid": "x", "data": "test2"}]`)
)

type mockProcessorPool struct {
	receivedParseables []parser.Parseable
}

func (mp *mockProcessorPool) StartListeners() {
}

func (mp *mockProcessorPool) Process(p parser.Parseable) {
	mp.receivedParseables = append(mp.receivedParseables, p)
}

func (mp *mockProcessorPool) Close() {}

func compressBytes(t *testing.T, compressionVersion byte, data []byte) []byte {
	var scratch bytes.Buffer
	compressor, err := flate.NewWriter(&scratch, flate.BestSpeed)
	require.Nil(t, err)
	_, err = compressor.Write(data)
	require.Nil(t, err)
	err = compressor.Close()
	require.Nil(t, err)
	return append([]byte{compressionVersion}, scratch.Bytes()...)
}

// Test that the glob is correctly decompressed.
func TestExpandGlobNominal(t *testing.T) {
	dp := NewPool(DeglobberPoolConfig{
		CompressionVersion: 1,
	})
	events, err := dp.expandGlob(compressBytes(t, 1, testMsg1))
	require.Nil(t, err)
	require.Equal(t, 1, len(events))
	assert.Equal(t, "x", events[0].Uuid)
	assert.Equal(t, "test", events[0].Data)
}

// Test that a bad decompression version causes an error.
func TestExpandGlobBadVersion(t *testing.T) {
	dp := NewPool(DeglobberPoolConfig{
		CompressionVersion: 2,
	})
	_, err := dp.expandGlob(compressBytes(t, 1, testMsg1))
	assert.NotNil(t, err)
}

// Test that crank processes events and dupes correctly.
func TestCrank(t *testing.T) {
	mpp := mockProcessorPool{}
	dc := cache.New(time.Minute, time.Minute)
	dp := NewPool(DeglobberPoolConfig{
		CompressionVersion: 1,
		Stats:              &statsd.NoopClient{},
		ProcessorPool:      &mpp,
		SpadeReporter:      &reporter.NoopReporter{},
		DuplicateCache:     dc,
		PoolSize:           1,
	})
	dp.Start()
	dp.Submit(compressBytes(t, 1, testMsg1))
	dp.Submit(compressBytes(t, 1, dupeMsg))
	dp.Submit(compressBytes(t, 1, testMsg2))
	dp.Close()
	require.Equal(t, 2, len(mpp.receivedParseables))
	var event, event2 spade.Event
	require.Nil(t, spade.Unmarshal(mpp.receivedParseables[0].Data(), &event))
	assert.Equal(t, "test", event.Data)
	require.Nil(t, spade.Unmarshal(mpp.receivedParseables[1].Data(), &event2))
	assert.Equal(t, "test3", event2.Data)
}
