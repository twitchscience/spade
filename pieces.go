package main

// I need a better name for this file

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

const (
	schemaReloadFrequency = 5 * time.Minute
	schemaRetryDelay      = 2 * time.Second
	parserPoolSize        = 10
	transformerPoolSize   = 10
)

func createSpadeReporter(stats reporter.StatsLogger, auditLogger *gologging.UploadLogger) reporter.Reporter {
	 return reporter.BuildSpadeReporter(
		&sync.WaitGroup{},
		[]reporter.Tracker{
			&reporter.SpadeStatsdTracker{Stats: stats},
			&reporter.SpadeUUIDTracker{Logger: auditLogger},
		},
	)
}

func createSpadeWriter(
	outputDir string,
	reporter reporter.Reporter,
	spadeUploaderPool *uploader.UploaderPool,
	blueprintUploaderPool *uploader.UploaderPool,
	maxLogBytes int64,
	maxLogAgeSecs int64,
) writer.SpadeWriter {
	w, err := writer.NewWriterController(
		outputDir,
		reporter,
		spadeUploaderPool,
		blueprintUploaderPool,
		maxLogBytes,
		maxLogAgeSecs,
	)

	if err != nil {
		logger.WithError(err).Fatal("Error creating spade writer")
	}

	return w
}

func createSchemaLoader(fetcher fetcher.ConfigFetcher, stats reporter.StatsLogger) *table_config.DynamicLoader {
	loader, err := table_config.NewDynamicLoader(fetcher, schemaReloadFrequency, schemaRetryDelay, stats)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create schema dynamic loader")
	}

	go loader.Crank()
	return loader
}

func createProcessorPool(loader transformer.ConfigLoader, reporter reporter.Reporter) *processor.SpadeProcessorPool {
	return processor.BuildProcessorPool(parserPoolSize, transformerPoolSize, loader, reporter)
}

func createConsumer(session *session.Session, stats statsd.Statter) *consumer.Consumer {
	consumer, err := consumer.New(session, stats, config.Consumer)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create consumer")
	}
	return consumer
}

func createGeoipUpdater(config *geoip.Config) *geoip.Updater {
	u := geoip.NewUpdater(time.Now(), transformer.GeoIpDB, *config)
	go u.UpdateLoop()
	return u
}

func createKinesisWriters(kinesis *kinesis.Kinesis, stats statsd.Statter) []writer.SpadeWriter {
	var writers []writer.SpadeWriter
	for _, c := range config.KinesisOutputs {
		w, err := writer.NewKinesisWriter(kinesis, stats, c)
		if err != nil {
			logger.WithError(err).
				WithField("stream_name", c.StreamName).
				Fatal("Failed to create Kinesis writer")
		}
		writers = append(writers, w)
	}

	return writers
}
