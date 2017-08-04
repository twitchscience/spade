package kinesisconfigs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

// A character guarenteed to not be in the key to delimit it
const kinesisKeyDelimiter = "|"

// DynamicLoader fetches configs on an interval, with stats on the fetching process.
type DynamicLoader struct {
	fetcher                fetcher.ConfigFetcher
	reloadTime             time.Duration
	retryDelay             time.Duration
	configs                []scoop_protocol.AnnotatedKinesisConfig
	closer                 chan bool
	stats                  reporter.StatsLogger
	multee                 writer.SpadeWriterManager
	kinesisWriterGenerator func(scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error)
	staticKeys             map[string]bool
}

// NewDynamicLoader returns a new DynamicLoader, performing the first fetch.
func NewDynamicLoader(
	fetcher fetcher.ConfigFetcher,
	reloadTime,
	retryDelay time.Duration,
	stats reporter.StatsLogger,
	multee writer.SpadeWriterManager,
	kinesisWriterGenerator func(scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error),
	staticConfigs []scoop_protocol.KinesisWriterConfig,
) (*DynamicLoader, error) {
	d := DynamicLoader{
		fetcher:                fetcher,
		reloadTime:             reloadTime,
		retryDelay:             retryDelay,
		configs:                []scoop_protocol.AnnotatedKinesisConfig{},
		closer:                 make(chan bool),
		stats:                  stats,
		multee:                 multee,
		kinesisWriterGenerator: kinesisWriterGenerator,
		staticKeys:             make(map[string]bool),
	}

	for _, staticConfig := range staticConfigs {
		d.staticKeys[buildStaticKey(staticConfig)] = true
	}

	config, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("kinesis config retry loop failed: %s", err)
	}
	d.configs = config

	err = d.startInitialWriters()
	if err != nil {
		return nil, fmt.Errorf("Error initializing kinesis writers: %s", err)
	}

	return &d, nil
}

// StartInitialWriters starts the initial batch of Kinesis Writers
func (d *DynamicLoader) startInitialWriters() error {
	// start the kinesis writers defined by Blueprint
	for _, newConfig := range d.configs {
		k := buildKey(newConfig)
		w, err := d.kinesisWriterGenerator(newConfig.SpadeConfig)
		if err != nil {
			return fmt.Errorf("Failed to create Kinesis writer %s at loader initialization: %s", k, err)
		}
		d.multee.Add(k, w)
	}
	return nil
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) ([]scoop_protocol.AnnotatedKinesisConfig, error) {
	var err error
	var config []scoop_protocol.AnnotatedKinesisConfig
	for i := 1; i <= n; i++ {
		config, err = d.pullConfigIn()
		if err == nil {
			return config, nil
		}
		time.Sleep(waitTime * time.Duration(i))
	}
	return nil, err
}

func (d *DynamicLoader) pullConfigIn() ([]scoop_protocol.AnnotatedKinesisConfig, error) {
	configReader, err := d.fetcher.Fetch()
	if err != nil {
		return nil, fmt.Errorf("could not fetch config from fetcher: %s", err)
	}

	b, err := ioutil.ReadAll(configReader)
	if err != nil {
		return nil, fmt.Errorf("could not read response from fetcher: %s", err)
	}
	var acfgs []scoop_protocol.AnnotatedKinesisConfig
	err = json.Unmarshal(b, &acfgs)
	if err != nil {
		return []scoop_protocol.AnnotatedKinesisConfig{}, fmt.Errorf("could not unmarshal annotated kinesis response: %s", err)
	}
	// validate
	for _, kinesisConfig := range acfgs {
		err = kinesisConfig.SpadeConfig.Validate()
		if err != nil {
			return nil, fmt.Errorf("could not validate kinesis configuration %s: %s", kinesisConfig.SpadeConfig.StreamName, err)
		}
	}

	// dedupe it so it has no overlap with the static set
	deduppedAcfgs := []scoop_protocol.AnnotatedKinesisConfig{}
	for _, kinesisConfig := range acfgs {
		if !d.staticKeys[buildStaticKey(kinesisConfig.SpadeConfig)] {
			deduppedAcfgs = append(deduppedAcfgs, kinesisConfig)
		}
	}

	return deduppedAcfgs, nil
}

// Close stops the DynamicLoader's fetching process.
func (d *DynamicLoader) Close() {
	d.closer <- true
}

func buildKey(c scoop_protocol.AnnotatedKinesisConfig) string {
	return strings.Join([]string{strconv.FormatInt(c.AWSAccount, 10), c.SpadeConfig.StreamType, c.SpadeConfig.StreamName}, kinesisKeyDelimiter)
}

func buildStaticKey(c scoop_protocol.KinesisWriterConfig) string {
	return strings.Join([]string{"static", c.StreamRole, c.StreamType, c.StreamName}, kinesisKeyDelimiter)
}

// Crank is a blocking function that refreshes the config on an interval.
func (d *DynamicLoader) Crank() {
	// Jitter reload
	tick := time.NewTicker(d.reloadTime + time.Duration(rand.Intn(100))*time.Millisecond)
	for {
		select {
		case <-d.closer:
			tick.Stop()
			return
		case <-tick.C:
			// can put a circuit breaker here.
			now := time.Now()
			newConfigs, err := d.retryPull(5, d.retryDelay)
			if err != nil {
				logger.WithError(err).Error("Failed to refresh kinesis config")
				d.stats.Timing("kinesisconfig.error", time.Since(now))
				continue
			}
			d.stats.Timing("kinesisconfig.success", time.Since(now))

			newConfigsLookup := make(map[string]*scoop_protocol.AnnotatedKinesisConfig)
			for i := range newConfigs {
				k := buildKey(newConfigs[i])
				newConfigsLookup[k] = &newConfigs[i]
			}
			for _, existingConfig := range d.configs {
				k := buildKey(existingConfig)
				newConfig, exist := newConfigsLookup[k]
				if !exist {
					// the new configs no longer have something we had before - it got dropped
					logger.Infof("Dropping writer %s", k)
					d.multee.Drop(k)
					continue
				}

				// remove the newConfig from the lookup
				delete(newConfigsLookup, k)

				// do nothing if version is the same
				if existingConfig.Version == newConfig.Version {
					continue
				}
				// the new config already exist - check for updated version
				if existingConfig.Version > newConfig.Version {
					// if incoming version is lower than existing, log error
					logger.
						WithField("key", k).
						WithField("existing_version", existingConfig.Version).
						WithField("new_version", newConfig.Version).
						Error("Incoming Kinesis config higher than previous version")
					continue
				}
				// if incoming version is higher than existing - replace the existing writer
				w, err := d.kinesisWriterGenerator(newConfig.SpadeConfig)
				if err != nil {
					logger.WithError(err).
						WithField("key", k).
						Error("Failed to create Kinesis writer when replacing kinesis writer")
				}
				logger.Infof("Replacing writer %s", k)
				d.multee.Replace(k, w)
			}

			// the leftovers are new configs we don't know about yet - add them
			for k, newConfig := range newConfigsLookup {
				w, err := d.kinesisWriterGenerator(newConfig.SpadeConfig)
				if err != nil {
					logger.WithError(err).
						WithField("key", k).
						Error("Failed to create Kinesis writer when adding kinesis writer")
				}
				logger.Infof("Adding writer %s", k)
				d.multee.Add(k, w)
			}

			// done with updating the actual writers, replace the config
			d.configs = newConfigs
		}
	}
}
