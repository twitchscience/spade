package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"time"

	"github.com/vrischmann/jsonutil"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/lookup"
)

var (
	configFilename = flag.String("config", "conf.json", "name of config file")
	printConfig    = flag.Bool("printConfig", false, "Print the config object after parsing?")
)

var config struct {
	// BlueprintSchemasURL is the url to blueprint schemas
	BlueprintSchemasURL string
	// BlueprintKinesisConfigsURL is the url to blueprint kinesisconfigs
	BlueprintKinesisConfigsURL string
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
	// Consumer is the config for the kinesis based event consumer
	Consumer consumer.Config
	// Geoip is the config for the geoip updater
	Geoip *geoip.Config
	// RollbarToken is our token to authenticate with Rollbar
	RollbarToken string
	// RollbarEnvironment is the environment we report we are running in to Rollbar
	RollbarEnvironment string

	// KinesisWriters contain a list of configs for KinesisWriters
	KinesisOutputs []scoop_protocol.KinesisWriterConfig

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
}

func loadConfig() {
	// Default values
	config.SchemaReloadFrequency = jsonutil.FromDuration(5 * time.Minute)
	config.SchemaRetryDelay = jsonutil.FromDuration(2 * time.Second)
	config.KinesisConfigReloadFrequency = jsonutil.FromDuration(10 * time.Minute)
	config.KinesisConfigRetryDelay = jsonutil.FromDuration(2 * time.Second)

	entry := logger.WithField("config_file", *configFilename)
	f, err := os.Open(*configFilename)
	if err != nil {
		entry.WithError(err).Fatal("Failed to load config")
	}

	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		entry.WithError(err).Fatal("Failed to decode JSON config")
	}

	err = validateConfig()
	if err != nil {
		entry.WithError(err).Fatal("Config is invalid")
	}

	if *printConfig {
		if b, err := json.MarshalIndent(config, "", "\t"); err != nil {
			entry.WithError(err).Error("Failed to marshal config")
		} else {
			entry.WithField("config", string(b)).Info("Configuration")
		}
	}
}

func checkNonempty(str string) error {
	if str == "" {
		return errors.New("empty string found for required config option")
	}
	return nil
}

func validateConfig() error {
	for _, str := range []string{
		config.BlueprintSchemasURL,
		config.BlueprintKinesisConfigsURL,
		config.AceBucketName,
		config.NonTrackedBucketName,
		config.Geoip.ConfigBucket,
		config.Geoip.IPCityKey,
		config.Geoip.IPASNKey,
		config.RollbarToken,
		config.RollbarEnvironment,
	} {
		if err := checkNonempty(str); err != nil {
			return err
		}
	}

	if !*replay {
		for _, str := range []string{
			config.ProcessorErrorTopicARN,
			config.AceTopicARN,
			config.AceErrorTopicARN,
			config.NonTrackedTopicARN,
			config.NonTrackedErrorTopicARN,
		} {
			if err := checkNonempty(str); err != nil {
				return err
			}
		}
	}

	for _, i := range []int64{
		config.MaxLogBytes,
		config.MaxLogAgeSecs,
		config.LRULifetimeSeconds,
		int64(config.Geoip.UpdateFrequencyMins),
		int64(config.Geoip.JitterSecs),
	} {
		if i <= 0 {
			return errors.New("nonpositive integer found in config, must provide positive integer")
		}
	}

	for _, c := range config.KinesisOutputs {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}
