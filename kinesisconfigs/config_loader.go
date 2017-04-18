package kinesisconfigs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

// StaticLoader is a static set of transformers.
type StaticLoader struct {
	configs []writer.AnnotatedKinesisConfig
}

// NewStaticLoader creates a StaticLoader from the given configs.
func NewStaticLoader(config []writer.AnnotatedKinesisConfig) *StaticLoader {
	return &StaticLoader{
		configs: config,
	}
}

// DynamicLoader fetches configs on an interval, with stats on the fetching process.
type DynamicLoader struct {
	fetcher    fetcher.ConfigFetcher
	reloadTime time.Duration
	retryDelay time.Duration
	configs    []writer.AnnotatedKinesisConfig
	lock       *sync.RWMutex
	closer     chan bool
	stats      reporter.StatsLogger
}

// NewDynamicLoader returns a new DynamicLoader, performing the first fetch.
func NewDynamicLoader(
	fetcher fetcher.ConfigFetcher,
	reloadTime,
	retryDelay time.Duration,
	stats reporter.StatsLogger,
) (*DynamicLoader, error) {
	d := DynamicLoader{
		fetcher:    fetcher,
		reloadTime: reloadTime,
		retryDelay: retryDelay,
		configs:    []writer.AnnotatedKinesisConfig{},
		lock:       &sync.RWMutex{},
		closer:     make(chan bool),
		stats:      stats,
	}
	config, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("kinesis config retry loop failed: %s", err)
	}
	d.configs = config
	return &d, nil
}

// GetKinesisConfigs returns the transformers for the given event.
func (s *StaticLoader) GetKinesisConfigs() []writer.AnnotatedKinesisConfig {
	return s.configs
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) ([]writer.AnnotatedKinesisConfig, error) {
	var err error
	var config []writer.AnnotatedKinesisConfig
	for i := 1; i < (n + 1); i++ {
		config, err = d.pullConfigIn()
		if err == nil {
			return config, fmt.Errorf("could not pull config: %s", err)
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return nil, err
}

func (d *DynamicLoader) pullConfigIn() ([]writer.AnnotatedKinesisConfig, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		return nil, fmt.Errorf("could not fetch config from fetcher: %s", err)
	}

	b, err := ioutil.ReadAll(configReader)
	if err != nil {
		return nil, fmt.Errorf("could not read response from fetcher: %s", err)
	}
	var acfgs []writer.AnnotatedKinesisConfig
	err = json.Unmarshal(b, &acfgs)
	if err != nil {
		return []writer.AnnotatedKinesisConfig{}, fmt.Errorf("could not unmarshal annotated kiness response: %s", err)
	}

	// validate
	for i := range acfgs {
		err = acfgs[i].SpadeConfig.Validate()
		if err != nil {
			return nil, fmt.Errorf("could not validate kinesis configuration %s: %s", acfgs[i].StreamName, err)
		}
	}
	return acfgs, nil
}

// Close stops the DynamicLoader's fetching process.
func (d *DynamicLoader) Close() {
	d.closer <- true
}

// GetKinesisConfigs returns all of the Kinesis streams currently configured.
func (d *DynamicLoader) GetKinesisConfigs() []writer.AnnotatedKinesisConfig {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.configs
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
			newConfig, err := d.retryPull(5, d.retryDelay)
			if err != nil {
				logger.WithError(err).Error("Failed to refresh kinesis config")
				d.stats.Timing("kinesisconfig.error", time.Since(now))
				continue
			}
			d.stats.Timing("kinesisconfig.success", time.Since(now))

			d.lock.Lock()
			d.configs = newConfig
			d.lock.Unlock()
		case <-d.closer:
			return
		}
	}
}
