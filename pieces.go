package main

// I need a better name for this file

import (
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cactus/go-statsd-client/statsd"
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

func createSpadeReporter(
	stats reporter.StatsLogger,
	auditLogger *gologging.UploadLogger,
) reporter.Reporter {
	r := reporter.BuildSpadeReporter(
		&sync.WaitGroup{},
		[]reporter.Tracker{
			&reporter.SpadeStatsdTracker{
				Stats: stats,
			},
			&reporter.SpadeUUIDTracker{
				Logger: auditLogger,
			},
		},
	)
	if r == nil {
		log.Fatal("Unable to build spade reporter")
	}

	return r
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
		log.Fatalf("Error creating spade writer: %s", err)
	}

	return w
}

func createSchemaLoader(
	fetcher fetcher.ConfigFetcher,
	stats reporter.StatsLogger,
) *table_config.DynamicLoader {
	loader, err := table_config.NewDynamicLoader(
		fetcher,
		schemaReloadFrequency,
		schemaRetryDelay,
		stats,
	)
	if err != nil {
		log.Fatalf("Error creating schema dynamic loader: %s", err)
	}
	go loader.Crank()

	return loader
}

func createProcessorPool(
	loader transformer.ConfigLoader,
	reporter reporter.Reporter,
) *processor.SpadeProcessorPool {
	processor := processor.BuildProcessorPool(
		parserPoolSize,
		transformerPoolSize,
		loader,
		reporter,
	)

	if processor == nil {
		log.Fatal("Error building processor pool")
	}

	return processor
}

func createConsumer(session *session.Session, stats statsd.Statter) *consumer.Consumer {
	consumer, err := consumer.New(session, stats, config.Consumer)
	if err != nil {
		log.Fatalf("Error creating consumer: %s", err)
	}
	return consumer
}

func createGeoipUpdater(config *geoip.Config) *geoip.Updater {
	u := geoip.NewUpdater(time.Now(), transformer.GeoIpDB, *config)
	if u == nil {
		log.Fatalf("Unable to create geoip updater")
	}
	go u.UpdateLoop()
	return u
}
