// Package deglobber decompresses and unglobs events and checks for duplicates.
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

// Pool turns byte streams into lists of events, then sends them to a processor.Pool. It also handles deduping globs.
type Pool struct {
	globs  chan []byte
	config PoolConfig
	wg     sync.WaitGroup
}

// PoolConfig collects the configuration information for a Pool.
type PoolConfig struct {
	ProcessorPool      processor.Pool
	Stats              statsd.Statter
	DuplicateCache     *cache.Cache
	PoolSize           int
	CompressionVersion byte
	ReplayMode         bool
}

// NewPool returns a pool for turning globs into lists of events.
func NewPool(config PoolConfig) *Pool {
	return &Pool{
		globs:  make(chan []byte, 1),
		config: config,
		wg:     sync.WaitGroup{},
	}
}

// Start starts the pool's goroutines.
func (dp *Pool) Start() {
	for i := 0; i < dp.config.PoolSize; i++ {
		dp.wg.Add(1)
		logger.Go(dp.crank)
	}
}

// Submit submits a glob to the pool for processing.
func (dp *Pool) Submit(glob []byte) {
	dp.globs <- glob
}

// Close stops all processing goroutines.
func (dp *Pool) Close() {
	close(dp.globs)
	dp.wg.Wait()
}

func (dp *Pool) expandGlob(glob []byte) (events []*spade.Event, err error) {
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

func (dp *Pool) processEvent(e *spade.Event) {
	now := time.Now()
	if err := dp.config.Stats.TimingDuration("event.age", now.Sub(e.ReceivedAt), 0.1); err != nil {
		logger.WithError(err).Error("Failed to submit timing")
	}
	d, err := spade.Marshal(e)
	if err != nil {
		logger.WithError(err).WithField("event", e).Error("Failed to marshal event")
	}
	dp.config.ProcessorPool.Process(&parseRequest{data: d, start: time.Now()})
}

func (dp *Pool) statHelper(stat string) {
	err := dp.config.Stats.Inc(stat, 1, 1.0)
	if err != nil {
		logger.WithError(err).Error("Stats update failed")
	}
}

func (dp *Pool) crank() {
	defer dp.wg.Done()
	for glob := range dp.globs {
		if dp.config.ReplayMode {
			// In replay mode, the "glob" is really only one uncompressed event
			var event spade.Event
			err := json.Unmarshal(glob, &event)
			if err != nil {
				logger.WithError(err).Error("Failed to unmarshall event")
				continue
			}
			dp.processEvent(&event)
			continue
		}

		events, err := dp.expandGlob(glob)
		if err != nil {
			logger.WithError(err).Error("Failed to expand glob")
			dp.statHelper("record.failures")
			continue
		}

		if len(events) == 0 {
			continue
		}

		dp.statHelper("record.count")

		uuid := events[0].Uuid
		if _, found := dp.config.DuplicateCache.Get(uuid); found {
			logger.WithField("uuid", uuid).Info("Ignoring duplicate UUID")
			dp.statHelper("record.dupe")
		} else {
			for _, e := range events {
				dp.processEvent(e)
			}
		}
		dp.config.DuplicateCache.Set(uuid, 0, cache.DefaultExpiration)
	}
}
