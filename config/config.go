package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/vrischmann/jsonutil"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/lookup"
)

// Config controls the processor's behavior.
type Config struct {
	// Directory for all spade output
	SpadeDir string
	// ConfigBucket is the bucket that the blueprint published config lives in
	ConfigBucket string
	// SchemasKey is the s3 key to the blueprint published schemas config
	SchemasKey string
	// KinesisConfigKey is the s3 key to the blueprint published schemas config
	KinesisConfigKey string
	// MetadataConfigKey is the s3 key to the blueprint published metadata config
	MetadataConfigKey string
	// ProcessorErrorTopicARN is the arn of the SNS topic for processor errors
	ProcessorErrorTopicARN string
	// AceTopicARN is the arn of the SNS topic for events going to Ace
	AceTopicARN string
	// AceErrorTopicARN is the arn of the SNS topic for errors when sending events to Ace
	AceErrorTopicARN string
	// NonTrackedTopicARN is the arn of the SNS topic for events not tracked in blueprint
	NonTrackedTopicARN string
	// NonTrackedErrorTopicARN is the arn of the SNS topic for errors when sending nontracked events
	NonTrackedErrorTopicARN string
	// AceBucketName is the name of the s3 bucket to put processed events into
	AceBucketName string
	// NonTrackedBucketName is the name of the s3 bucket to put nontracked events into
	NonTrackedBucketName string
	// MaxLogBytes is the max number of log bytes before file rotation
	MaxLogBytes int64
	// MaxLogAgeSecs is the max number of seconds between log rotations
	MaxLogAgeSecs int64
	// NontrackedMaxLogAgeSecs is the max number of seconds between nontracked log rotations
	NontrackedMaxLogAgeSecs int64
	// Consumer is the config for the kinesis based event consumer
	Consumer consumer.Config
	// Geoip is the config for the geoip updater
	Geoip *geoip.Config
	// RollbarToken is our token to authenticate with Rollbar
	RollbarToken string
	// RollbarEnvironment is the environment we report we are running in to Rollbar
	RollbarEnvironment string

	// KinesisOutputs contains configs for KinesisWriters.
	KinesisOutputs []scoop_protocol.KinesisWriterConfig
	// KinesisWriterErrorsBeforeThrottling is the number of errors each Kinesis writer is permitted
	// to write to Rollbar before throttling kicks in.  Set to 0 to start throttling immediately.
	KinesisWriterErrorsBeforeThrottling int
	// KinesisWriterErrorThrottlePeriodSeconds is the error throttle period for each Kinesis writer
	// (i.e. once throttling switches on, only one error will be sent to Rollbar per period).
	// Set to 0 to turn throttling off and send all errors to Rollbar.
	KinesisWriterErrorThrottlePeriodSeconds int64

	// JSONValueFetchers is a map of id to JSONValueFetcherConfigs
	JSONValueFetchers map[string]lookup.JSONValueFetcherConfig

	// TransformerCacheCluster contains the config required to instantiate a cache for transformers
	TransformerCacheCluster elastimemcache.Config

	// TransformerFetchers is a map of transformer id to value fetcher id
	TransformerFetchers map[string]string

	// LRULifetimeSeconds is the lifetime of an item in the local cache, in seconds.
	LRULifetimeSeconds int64

	// How often to load table schemas from Blueprint.
	SchemaReloadFrequency jsonutil.Duration
	// How long to sleep if there's an error loading table schemas from Blueprint.
	SchemaRetryDelay jsonutil.Duration
	// How often to load kinesis configs from Blueprint.
	KinesisConfigReloadFrequency jsonutil.Duration
	// How long to sleep if there's an error loading kinesis configs from Blueprint.
	KinesisConfigRetryDelay jsonutil.Duration
	// How often to load event metadata from Blueprint.
	EventMetadataReloadFrequency jsonutil.Duration
	// How long to sleep if there's an error loading event metadata from Blueprint.
	EventMetadataRetryDelay jsonutil.Duration

	// Host:port to send statsd events to
	StatsdHostport string
	// Prefix for statsd metrics
	StatsdPrefix string
}

// LoadConfig loads the config object from the file, performing validations.
func LoadConfig(configFilename string, replay bool) (*Config, error) {
	cfg := Config{}
	// Default values
	cfg.SchemaReloadFrequency = jsonutil.FromDuration(5 * time.Minute)
	cfg.SchemaRetryDelay = jsonutil.FromDuration(2 * time.Second)
	cfg.KinesisConfigReloadFrequency = jsonutil.FromDuration(10 * time.Minute)
	cfg.KinesisConfigRetryDelay = jsonutil.FromDuration(2 * time.Second)
	cfg.EventMetadataReloadFrequency = jsonutil.FromDuration(5 * time.Minute)
	cfg.EventMetadataRetryDelay = jsonutil.FromDuration(2 * time.Second)

	f, err := os.Open(configFilename)
	if err != nil {
		return nil, fmt.Errorf("opening config: %v", err)
	}

	err = json.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("decoding config: %v", err)
	}

	err = validateConfig(cfg, replay)
	if err != nil {
		return nil, fmt.Errorf("validating config: %v", err)
	}

	return &cfg, nil
}

func checkNonempty(str string) error {
	if str == "" {
		return errors.New("empty string found for required config option")
	}
	return nil
}

func validateConfig(cfg Config, replay bool) error {
	for _, str := range []string{
		cfg.ConfigBucket,
		cfg.SchemasKey,
		cfg.KinesisConfigKey,
		cfg.MetadataConfigKey,
		cfg.AceBucketName,
		cfg.NonTrackedBucketName,
		cfg.Geoip.ConfigBucket,
		cfg.Geoip.IPCity.Key,
		cfg.Geoip.IPCity.Path,
		cfg.Geoip.IPASN.Key,
		cfg.Geoip.IPASN.Path,
		cfg.RollbarToken,
		cfg.RollbarEnvironment,
	} {
		if err := checkNonempty(str); err != nil {
			return err
		}
	}

	if !replay {
		for _, str := range []string{
			cfg.ProcessorErrorTopicARN,
			cfg.AceTopicARN,
			cfg.AceErrorTopicARN,
			cfg.NonTrackedTopicARN,
			cfg.NonTrackedErrorTopicARN,
		} {
			if err := checkNonempty(str); err != nil {
				return err
			}
		}
	}

	for _, i := range []int64{
		cfg.MaxLogBytes,
		cfg.MaxLogAgeSecs,
		cfg.LRULifetimeSeconds,
		int64(cfg.Geoip.UpdateFrequencyMins),
		int64(cfg.Geoip.JitterSecs),
	} {
		if i <= 0 {
			return errors.New("nonpositive integer found in config, must provide positive integer")
		}
	}

	for _, i := range []int64{
		cfg.KinesisWriterErrorThrottlePeriodSeconds,
		int64(cfg.KinesisWriterErrorsBeforeThrottling),
	} {
		if i < 0 {
			return errors.New(
				"negative integer found in config, must provide nonnegative integer or leave blank")
		}
	}

	for _, c := range cfg.KinesisOutputs {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}
