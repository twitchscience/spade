package main

import (
	"encoding/json"
	"errors"
	"os"
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
	// EdgeQueue is the name of the SQS queue to listen to for events from the edge
	EdgeQueue string
	// MaxLogBytes is the max number of log bytes before file rotation
	MaxLogBytes int64
	// MaxLogAgeSecs is the max number of seconds between log rotations
	MaxLogAgeSecs int64
}

func loadConfig(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	p := json.NewDecoder(f)
	err = p.Decode(&config)
	if err != nil {
		return err
	}

	return validateConfig()
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
		config.EdgeQueue,
	} {
		if str == "" {
			return errors.New("Empty string found for required config option.")
		}
	}
	for _, i := range []int64{config.SQSPollInterval, config.MaxLogBytes, config.MaxLogAgeSecs} {
		if i <= 0 {
			return errors.New("Nonpositive integer found in config, must provide positive integer.")
		}
	}
	return nil
}
