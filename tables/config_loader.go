package tables

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/transformer"
)

// StaticLoader is a static set of transformers and versions.
type StaticLoader struct {
	configs  map[string][]transformer.RedshiftType
	versions map[string]int
}

// NewStaticLoader creates a StaticLoader from the given configs and versions.
func NewStaticLoader(config map[string][]transformer.RedshiftType, versions map[string]int) *StaticLoader {
	return &StaticLoader{
		configs:  config,
		versions: versions,
	}
}

// DynamicLoader fetches configs on an interval, with stats on the fetching process.
type DynamicLoader struct {
	fetcher    fetcher.ConfigFetcher
	reloadTime time.Duration
	retryDelay time.Duration
	configs    map[string][]transformer.RedshiftType
	versions   map[string]int
	lock       *sync.RWMutex
	closer     chan bool
	stats      reporter.StatsLogger
	tConfigs   map[string]transformer.MappingTransformerConfig
	geoip      geoip.GeoLookup
}

// NewDynamicLoader returns a new DynamicLoader, performing the first fetch.
func NewDynamicLoader(
	fetcher fetcher.ConfigFetcher,
	reloadTime,
	retryDelay time.Duration,
	stats reporter.StatsLogger,
	tConfigs map[string]transformer.MappingTransformerConfig,
	geoip geoip.GeoLookup,
) (*DynamicLoader, error) {
	d := DynamicLoader{
		fetcher:    fetcher,
		reloadTime: reloadTime,
		retryDelay: retryDelay,
		configs:    make(map[string][]transformer.RedshiftType),
		versions:   make(map[string]int),
		lock:       &sync.RWMutex{},
		closer:     make(chan bool),
		stats:      stats,
		tConfigs:   tConfigs,
		geoip:      geoip,
	}
	config, versions, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, err
	}
	d.configs = config
	d.versions = versions
	return &d, nil
}

// GetColumnsForEvent returns the transformers for the given event.
func (s *StaticLoader) GetColumnsForEvent(eventName string) ([]transformer.RedshiftType, error) {
	if transformArray, exists := s.configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, transformer.ErrNotTracked{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

// GetVersionForEvent returns the current version of the given event.
func (s *StaticLoader) GetVersionForEvent(eventName string) int {
	if version, exists := s.versions[eventName]; exists {
		return version
	}
	return 0
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) (map[string][]transformer.RedshiftType, map[string]int, error) {
	var err error
	var config map[string][]transformer.RedshiftType
	var versions map[string]int
	for i := 1; i < (n + 1); i++ {
		config, versions, err = d.pullConfigIn()
		if err == nil {
			return config, versions, nil
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return nil, nil, err
}

func (d *DynamicLoader) pullConfigIn() (map[string][]transformer.RedshiftType, map[string]int, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		return nil, nil, err
	}

	tables, err := LoadConfig(configReader)
	if err != nil {
		return nil, nil, err
	}

	newConfigs, newVersions, err := tables.CompileForParsing(d.tConfigs, d.geoip)
	if err != nil {
		return nil, nil, err
	}
	return newConfigs, newVersions, nil
}

// Close stops the DynamicLoader's fetching process.
func (d *DynamicLoader) Close() {
	d.closer <- true
}

// GetColumnsForEvent returns the transformers for the given event.
func (d *DynamicLoader) GetColumnsForEvent(eventName string) ([]transformer.RedshiftType, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if transformArray, exists := d.configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, transformer.ErrNotTracked{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

// GetVersionForEvent returns the current version of the given event.
func (d *DynamicLoader) GetVersionForEvent(eventName string) int {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if version, exists := d.versions[eventName]; exists {
		return version
	}
	return 0
}

// Crank is a blocking function that refreshes the config on an interval.
func (d *DynamicLoader) Crank() {
	// Jitter reload
	tick := time.NewTicker(d.reloadTime + time.Duration(rand.Intn(100))*time.Millisecond)
	for {
		select {
		case <-tick.C:
			// can put a circuit breaker here.
			now := time.Now()
			newConfig, newVersions, err := d.retryPull(5, d.retryDelay)
			if err != nil {
				logger.WithError(err).Error("Failed to refresh config")
				d.stats.Timing("config.error", time.Since(now))
				continue
			}
			d.stats.Timing("config.success", time.Since(now))

			d.lock.Lock()
			d.configs = newConfig
			d.versions = newVersions
			d.lock.Unlock()
		case <-d.closer:
			tick.Stop()
			return
		}
	}
}
