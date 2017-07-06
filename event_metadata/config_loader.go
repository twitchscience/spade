package eventmetadata

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/transformer"
)

// StaticLoader is a static set of transformers and versions.
type StaticLoader struct {
	configs scoop_protocol.EventMetadataConfig
}

// NewStaticLoader creates a StaticLoader from the given event metadata config
func NewStaticLoader(config scoop_protocol.EventMetadataConfig) *StaticLoader {
	return &StaticLoader{
		configs: config,
	}
}

// DynamicLoader fetches configs on an interval, with stats on the fetching process.
type DynamicLoader struct {
	fetcher    fetcher.ConfigFetcher
	reloadTime time.Duration
	retryDelay time.Duration
	configs    scoop_protocol.EventMetadataConfig

	closer chan bool
	stats  reporter.StatsLogger
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
		configs:    scoop_protocol.EventMetadataConfig{},
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

// GetMetadataValueByType returns the metadata value given an eventName and metadataType
func (s *StaticLoader) GetMetadataValueByType(eventName string, metadataType string) (string, error) {
	if eventMetadata, found := s.configs.Metadata[eventName]; found {
		if metadataRow, exists := eventMetadata[metadataType]; exists {
			return metadataRow.MetadataValue, nil
		}
		return "", nil
	}
	return "", transformer.ErrNotTracked{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

// GetMetadataValueByType returns the metadata value given an eventName and metadataType
func (d *DynamicLoader) GetMetadataValueByType(eventName string, metadataType string) (string, error) {
	// if metadataType != string(scoop_protocol.COMMENT) && metadataType != string(scoop_protocol.EDGE_TYPE) {
	// 	return "", transformer.ErrInvalidMetadataType{
	// 		What: fmt.Sprintf("%s is not a valid metadata type", metadataType),
	// 	}
	// }

	if eventMetadata, found := d.configs.Metadata[eventName]; found {
		if metadataRow, exists := eventMetadata[metadataType]; exists {
			return metadataRow.MetadataValue, nil
		}
	}
	return "", transformer.ErrNotTracked{
		What: fmt.Sprintf("%s is not being tracked", eventName),
	}
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) (scoop_protocol.EventMetadataConfig, error) {
	var err error
	var config scoop_protocol.EventMetadataConfig
	for i := 1; i <= n; i++ {
		config, err = d.pullConfigIn()
		if err == nil {
			return config, nil
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return config, err
}

func (d *DynamicLoader) pullConfigIn() (scoop_protocol.EventMetadataConfig, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		var config scoop_protocol.EventMetadataConfig
		return config, err
	}

	b, err := ioutil.ReadAll(configReader)
	if err != nil {
		var config scoop_protocol.EventMetadataConfig
		return config, err
	}
	cfgs := scoop_protocol.EventMetadataConfig{
		Metadata: make(map[string](map[string]scoop_protocol.EventMetadataRow)),
	}
	err = json.Unmarshal(b, &cfgs.Metadata)
	if err != nil {
		return scoop_protocol.EventMetadataConfig{}, err
	}
	return cfgs, nil
}

// Close stops the DynamicLoader's fetching process.
func (d *DynamicLoader) Close() {
	d.closer <- true
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
				logger.WithError(err).Error("Failed to refresh config")
				d.stats.Timing("config.error", time.Since(now))
				continue
			}
			d.stats.Timing("config.success", time.Since(now))
			d.configs = newConfig
		case <-d.closer:
			return
		}
	}
}
