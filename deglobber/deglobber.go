// deglobber uncompresses and unglobs events and checks for duplicates
package deglobber

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	cache "github.com/patrickmn/go-cache"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/processor"
)

type parseRequest struct {
	data  []byte
	start time.Time
}

func (p *parseRequest) Data() []byte {
	return p.data
}

func (p *parseRequest) StartTime() time.Time {
	return p.start
}

// Pool receives byte streams, transforms them, and sends them downstream.
type Pool interface {
	Submit([]byte)
	Close()
}

// DeglobberPool is a Pool that turns byte streams into lists of events, then sends them
// to a processor.Pool. It also handles deduping globs.
type DeglobberPool struct {
	globs  chan []byte
	config DeglobberPoolConfig
	wg     sync.WaitGroup
}

type DeglobberPoolConfig struct {
	ProcessorPool      processor.Pool
	Stats              statsd.Statter
	DuplicateCache     *cache.Cache
	PoolSize           int
	CompressionVersion byte
}

// NewPool returns a pool for turning globs into lists of events.
func NewPool(config DeglobberPoolConfig) *DeglobberPool {
	dp := DeglobberPool{
		globs:  make(chan []byte, 1),
		config: config,
		wg:     sync.WaitGroup{},
	}
	return &dp
}

// Start starts the pool's goroutines.
func (dp *DeglobberPool) Start() {
	for i := 0; i < dp.config.PoolSize; i++ {
		dp.wg.Add(1)
		logger.Go(dp.crank)
	}
}

// Submit submits a glob to the pool for processing.
func (dp *DeglobberPool) Submit(glob []byte) {
	dp.globs <- glob
}

// Close stops all processing goroutines.
func (dp *DeglobberPool) Close() {
	close(dp.globs)
	dp.wg.Wait()
}

func (dp *DeglobberPool) expandGlob(glob []byte) (events []*spade.Event, err error) {
	compressed := bytes.NewBuffer(glob)

	v, err := compressed.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading version byte: %s", err)
	}
	if v != dp.config.CompressionVersion {
		return nil, fmt.Errorf("unknown version: got %v expected %v", v, dp.config.CompressionVersion)
	}

	deflator := flate.NewReader(compressed)
	defer func() {
		if cerr := deflator.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("error closing glob reader: %v", cerr)
		}
	}()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, deflator)
	if err != nil {
		return nil, fmt.Errorf("error decompressing: %v", err)
	}

	err = json.Unmarshal(decompressed.Bytes(), &events)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling: %v", err)
	}

	return
}

func (dp *DeglobberPool) crank() {
	defer dp.wg.Done()
	for glob := range dp.globs {
		events, err := dp.expandGlob(glob)
		if err != nil {
			logger.WithError(err).Error("Failed to expand glob")
			continue
		}

		if len(events) == 0 {
			continue
		}

		_ = dp.config.Stats.Inc("record.count", 1, 1.0)
		uuid := events[0].Uuid
		if _, found := dp.config.DuplicateCache.Get(uuid); found {
			logger.WithField("uuid", uuid).Info("Ignoring duplicate UUID")
			_ = dp.config.Stats.Inc("record.dupe", 1, 1.0)
		} else {
			for _, e := range events {
				now := time.Now()
				_ = dp.config.Stats.TimingDuration("record.age", now.Sub(e.ReceivedAt), 1.0)
				d, _ := spade.Marshal(e)
				dp.config.ProcessorPool.Process(&parseRequest{data: d, start: time.Now()})
			}
		}
		dp.config.DuplicateCache.Set(uuid, 0, cache.DefaultExpiration)
	}
}
