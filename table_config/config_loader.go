package table_config

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/TwitchScience/spade/config_fetcher/fetcher"
	"github.com/TwitchScience/spade/reporter"
	"github.com/TwitchScience/spade/transformer"
)

type StaticLoader struct {
	configs map[string][]transformer.RedshiftType
}

func NewStaticLoader(config map[string][]transformer.RedshiftType) *StaticLoader {
	return &StaticLoader{
		configs: config,
	}
}

type DynamicLoader struct {
	fetcher    fetcher.ConfigFetcher
	reloadTime time.Duration
	configs    map[string][]transformer.RedshiftType
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
		lock:       &sync.RWMutex{},
		closer:     make(chan bool),
		stats:      stats,
	}
	config, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, err
	}
	d.configs = config
	return &d, nil
}

func (s *StaticLoader) GetColumnsForEvent(eventName string) ([]transformer.RedshiftType, error) {
	if transformArray, exists := s.configs[eventName]; exists {
		return transformArray, nil
	}
	return nil, transformer.NotTrackedError{fmt.Sprintf("%s is not being tracked", eventName)}
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) (map[string][]transformer.RedshiftType, error) {
	var err error
	var config map[string][]transformer.RedshiftType
	for i := 1; i < (n + 1); i++ {
		config, err = d.pullConfigIn()
		if err == nil {
			return config, nil
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return nil, err
}

func (d *DynamicLoader) pullConfigIn() (map[string][]transformer.RedshiftType, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		return nil, err
	}

	tables, err := LoadConfig(configReader)
	if err != nil {
		return nil, err
	}

	newConfigs, err := tables.CompileForParsing()
	if err != nil {
		return nil, err
	}
	return newConfigs, nil
}

func (d *DynamicLoader) Crank() {
	// Jitter reload
	tick := time.NewTicker(d.reloadTime + time.Duration(rand.Intn(100))*time.Millisecond)
	for {
		select {
		case <-tick.C:
			// can put a circuit breaker here.
			now := time.Now()
			newConfig, err := d.pullConfigIn()
			if err != nil {
				log.Printf("Failed to refresh config: %v\n", err)
				d.stats.Timing("config.error", time.Now().Sub(now))
				continue
			}
			d.stats.Timing("config.success", time.Now().Sub(now))

			d.lock.Lock()
			d.configs = newConfig
			d.lock.Unlock()
		case <-d.closer:
			return
		}
	}
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
	return nil, transformer.NotTrackedError{fmt.Sprintf("%s is not being tracked", eventName)}
}
