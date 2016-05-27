package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"

	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
)

var (
	configFilename = flag.String("config", "conf.json", "name of config file")
	printConfig    = flag.Bool("printConfig", false, "Print the config object after parsing?")
)

var config struct {
	// BlueprintSchemasURL is the url to blueprint schemas
	BlueprintSchemasURL string
	// SQSPollInterval is how often should we poll SQS in seconds
	SQSPollInterval int64
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
	// AuditBucketName is the name of the s3 bucket to put audits into
	AuditBucketName string
	// MaxLogBytes is the max number of log bytes before file rotation
	MaxLogBytes int64
	// MaxLogAgeSecs is the max number of seconds between log rotations
	MaxLogAgeSecs int64
	// Consumer is the config for the kinesis based event consumer
	Consumer consumer.Config
	// Geoip is the config for the geoip updater
	Geoip *geoip.Config
}

func loadConfig() {
	f, err := os.Open(*configFilename)
	if err != nil {
		log.Fatalf("Unable to load config from %s: %s", *configFilename, err)
	}

	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		log.Fatalf("Unable to decode json config from %s: %s", *configFilename, err)
	}

	err = validateConfig()
	if err != nil {
		log.Fatalf("Config from %s is invalid: %s", *configFilename, err)
	}

	if *printConfig {
		b, _ := json.MarshalIndent(config, "", "\t")
		log.Printf("\n%s", string(b))
	}
}

func validateConfig() error {
	for _, str := range []string{
		config.BlueprintSchemasURL,
		config.ProcessorErrorTopicARN,
		config.AceTopicARN,
		config.AceErrorTopicARN,
		config.NonTrackedTopicARN,
		config.NonTrackedErrorTopicARN,
		config.AceBucketName,
		config.NonTrackedBucketName,
		config.AuditBucketName,
		config.Geoip.ConfigBucket,
		config.Geoip.IpCityKey,
		config.Geoip.IpASNKey,
	} {
		if str == "" {
			return errors.New("Empty string found for required config option.")
		}
	}
	for _, i := range []int64{
		config.SQSPollInterval,
		config.MaxLogBytes,
		config.MaxLogAgeSecs,
		int64(config.Geoip.UpdateFrequencyMins),
		int64(config.Geoip.JitterSecs),
	} {
		if i <= 0 {
			return errors.New("Nonpositive integer found in config, must provide positive integer.")
		}
	}
	return nil
}
