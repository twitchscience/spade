package kinesisconfigs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

// A character guarenteed to not be in the key to delimit it
const kinesisKeyDelimiter = "|"

// StaticLoader is a static set of transformers.
type StaticLoader struct {
	configs []scoop_protocol.AnnotatedKinesisConfig
}

// NewStaticLoader creates a StaticLoader from the given configs.
func NewStaticLoader(config []scoop_protocol.AnnotatedKinesisConfig) *StaticLoader {
	return &StaticLoader{
		configs: config,
	}
}

// DynamicLoader fetches configs on an interval, with stats on the fetching process.
type DynamicLoader struct {
	fetcher                fetcher.ConfigFetcher
	reloadTime             time.Duration
	retryDelay             time.Duration
	configs                []scoop_protocol.AnnotatedKinesisConfig
	lock                   *sync.RWMutex
	closer                 chan bool
	stats                  reporter.StatsLogger
	multee                 writer.SpadeWriterManager
	session                *session.Session
	kinesisWriterGenerator func(*session.Session, statsd.Statter, scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error)
}

// NewDynamicLoader returns a new DynamicLoader, performing the first fetch.
func NewDynamicLoader(
	fetcher fetcher.ConfigFetcher,
	reloadTime,
	retryDelay time.Duration,
	stats reporter.StatsLogger,
	multee writer.SpadeWriterManager,
	session *session.Session,
	kinesisWriterGenerator func(*session.Session, statsd.Statter, scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error),
) (*DynamicLoader, error) {
	d := DynamicLoader{
		fetcher:                fetcher,
		reloadTime:             reloadTime,
		retryDelay:             retryDelay,
		configs:                []scoop_protocol.AnnotatedKinesisConfig{},
		lock:                   &sync.RWMutex{},
		closer:                 make(chan bool),
		stats:                  stats,
		multee:                 multee,
		session:                session,
		kinesisWriterGenerator: kinesisWriterGenerator,
	}
	config, err := d.retryPull(5, retryDelay)
	if err != nil {
		return nil, fmt.Errorf("kinesis config retry loop failed: %s", err)
	}
	d.configs = config

	err = d.StartInitialWriters()
	if err != nil {
		return nil, fmt.Errorf("Error initializing kinesis writers: %s", err)
	}

	return &d, nil
}

// StartInitialWriters starts the initial batch of Kinesis Writers
func (d *DynamicLoader) StartInitialWriters() error {
	// start the kinesis writers defined by Blueprint
	for _, newConfig := range d.configs {
		k := buildKey(newConfig)
		w, err := d.kinesisWriterGenerator(d.session, d.stats.GetStatter(), newConfig.SpadeConfig)
		if err != nil {
			return fmt.Errorf("Failed to create Kinesis writer %s at loader initialization: %s", k, err)
		}
		d.multee.Add(k, w)
	}
	return nil
}

// GetKinesisConfigs returns the transformers for the given event.
func (s *StaticLoader) GetKinesisConfigs() []scoop_protocol.AnnotatedKinesisConfig {
	return s.configs
}

func (d *DynamicLoader) retryPull(n int, waitTime time.Duration) ([]scoop_protocol.AnnotatedKinesisConfig, error) {
	var err error
	var config []scoop_protocol.AnnotatedKinesisConfig
	for i := 1; i < (n + 1); i++ {
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
	for i := range acfgs {
		err = acfgs[i].SpadeConfig.Validate()
		if err != nil {
			return nil, fmt.Errorf("could not validate kinesis configuration %s: %s", acfgs[i].SpadeConfig.StreamName, err)
		}
	}
	return acfgs, nil
}

// Close stops the DynamicLoader's fetching process.
func (d *DynamicLoader) Close() {
	d.closer <- true
}

// GetKinesisConfigs returns all of the Kinesis streams currently configured.
func (d *DynamicLoader) GetKinesisConfigs() []scoop_protocol.AnnotatedKinesisConfig {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.configs
}

func buildKey(c scoop_protocol.AnnotatedKinesisConfig) string {
	return strings.Join([]string{strconv.FormatInt(c.AWSAccount, 10), c.SpadeConfig.StreamType, c.SpadeConfig.StreamName}, kinesisKeyDelimiter)
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
			newConfigs, err := d.retryPull(5, d.retryDelay)
			if err != nil {
				logger.WithError(err).Error("Failed to refresh kinesis config")
				d.stats.Timing("kinesisconfig.error", time.Since(now))
				continue
			}
			d.stats.Timing("kinesisconfig.success", time.Since(now))

			d.lock.Lock() // this locked block starts here...
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
				} else {
					// the new config already exist - check for updated version
					if existingConfig.Version > newConfig.Version {
						// if incoming version is lower than existing, log error
						logger.
							WithField("key", k).
							WithField("existing_version", existingConfig.Version).
							WithField("new_version", newConfig.Version).
							Error("Incoming Kinesis config higher than previous version")
					} else if existingConfig.Version < newConfig.Version {
						// if incoming version is higher than existing - replace the existing writer
						w, err := d.kinesisWriterGenerator(d.session, d.stats.GetStatter(), newConfig.SpadeConfig)
						if err != nil {
							logger.WithError(err).
								WithField("key", k).
								Error("Failed to create Kinesis writer when replacing kinesis writer")
						}
						logger.Infof("Replacing writer %s", k)
						d.multee.Replace(k, w)
					}
					// do nothing if version is the same

					// remove the newConfig from the lookup
					delete(newConfigsLookup, k)
				}
			}

			// the leftovers are new configs we don't know about yet - add them
			for k, newConfig := range newConfigsLookup {
				w, err := d.kinesisWriterGenerator(d.session, d.stats.GetStatter(), newConfig.SpadeConfig)
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

			d.lock.Unlock() // ...and ends here
		case <-d.closer:
			return
		}
	}
}
