package table_config

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/transformer"
)

type StaticLoader struct {
	configs  map[string][]transformer.RedshiftType
	versions map[string]int
}

func NewStaticLoader(config map[string][]transformer.RedshiftType, versions map[string]int) *StaticLoader {
	return &StaticLoader{
		configs:  config,
		versions: versions,
	}
}

type DynamicLoader struct {
	fetcher    fetcher.ConfigFetcher
	reloadTime time.Duration
	configs    map[string][]transformer.RedshiftType
	versions   map[string]int
	lock       *sync.RWMutex
	closer     chan bool
	stats      reporter.StatsLogger
}

func NewDynamicLoader(
	fetcher fetcher.ConfigFetcher,
	reloadTime,
	retryDelay time.Duration,
	stats reporter.StatsLogger,
) (*DynamicLoader, error) {
	d := DynamicLoader{
		fetcher:    fetcher,
		reloadTime: reloadTime,
		configs:    make(map[string][]transformer.RedshiftType),
		versions:   make(map[string]int),
		lock:       &sync.RWMutex{},
		closer:     make(chan bool),
		stats:      stats,
	}
	config, versions, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, err
	}
	d.configs = config
	d.versions = versions
	return &d, nil
}

func (s *StaticLoader) GetColumnsForEvent(eventName string) ([]transformer.RedshiftType, error) {
	if transformArray, exists := s.configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, transformer.NotTrackedError{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

func (d *StaticLoader) GetVersionForEvent(eventName string) int {
	if version, exists := d.versions[eventName]; exists {
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

	newConfigs, newVersions, err := tables.CompileForParsing()
	if err != nil {
		return nil, nil, err
	}
	return newConfigs, newVersions, nil
}

func (d *DynamicLoader) Close() {
	d.closer <- true
}

func (d *DynamicLoader) GetColumnsForEvent(eventName string) ([]transformer.RedshiftType, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if transformArray, exists := d.configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, transformer.NotTrackedError{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

func (d *DynamicLoader) GetVersionForEvent(eventName string) int {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if version, exists := d.versions[eventName]; exists {
		return version
	}
	return 0
}

func (d *DynamicLoader) Crank() {
	// Jitter reload
	tick := time.NewTicker(d.reloadTime + time.Duration(rand.Intn(100))*time.Millisecond)
	for {
		select {
		case <-tick.C:
			// can put a circuit breaker here.
			now := time.Now()
			newConfig, newVersions, err := d.pullConfigIn()
			if err != nil {
				log.Printf("Failed to refresh config: %v\n", err)
				d.stats.Timing("config.error", time.Now().Sub(now))
				continue
			}
			d.stats.Timing("config.success", time.Now().Sub(now))

			d.lock.Lock()
			d.configs = newConfig
			d.versions = newVersions
			d.lock.Unlock()
		case <-d.closer:
			return
		}
	}
}
