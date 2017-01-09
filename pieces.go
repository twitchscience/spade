package main

// I need a better name for this file

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/reporter"
	tableConfig "github.com/twitchscience/spade/tables"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

const (
	schemaReloadFrequency = 5 * time.Minute
	schemaRetryDelay      = 2 * time.Second
)

func createStatsdStatter() statsd.Statter {
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	if statsdHostport == "" {
		logger.Warning("STATSD_HOSTPORT is empty, using noop statsd connection")
		stats, err := statsd.NewNoop()
		if err != nil {
			logger.WithError(err).Fatal("Failed to create noop statsd connection (!?)")
		}
		return stats
	}

	stats, err := statsd.New(statsdHostport, *statsPrefix)
	if err != nil {
		logger.WithError(err).Fatal("Statsd configuration error")
	}
	logger.WithField("statsd_host_port", statsdHostport).Info("Connected to statsd")
	return stats
}

func startELBHealthCheckListener() {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	logger.Go(func() {
		err := http.ListenAndServe(net.JoinHostPort("", "8080"), healthMux)
		logger.WithError(err).Error("Failure listening to port 8080 with healthMux")
	})
}

func createSpadeReporter(stats reporter.StatsLogger) reporter.Reporter {
	return reporter.BuildSpadeReporter(
		[]reporter.Tracker{&reporter.SpadeStatsdTracker{Stats: stats}})
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

func createSchemaLoader(fetcher fetcher.ConfigFetcher, stats reporter.StatsLogger) *tableConfig.DynamicLoader {
	loader, err := tableConfig.NewDynamicLoader(fetcher, schemaReloadFrequency, schemaRetryDelay, stats)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create schema dynamic loader")
	}

	logger.Go(loader.Crank)
	return loader
}

func createPipe(session *session.Session, stats statsd.Statter, replay bool) consumer.ResultPipe {
	if replay {
		return consumer.NewStandardInputPipe()
	}

	consumer, err := consumer.NewKinesisPipe(session, stats, config.Consumer)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create consumer")
	}
	return consumer
}

func createGeoipUpdater(config *geoip.Config) *geoip.Updater {
	u := geoip.NewUpdater(time.Now(), transformer.GeoIPDB, *config, s3.New(session.New(&aws.Config{})))
	logger.Go(u.UpdateLoop)
	return u
}

func createKinesisWriters(session *session.Session, stats statsd.Statter) []writer.SpadeWriter {
	var writers []writer.SpadeWriter
	for _, c := range config.KinesisOutputs {
		w, err := writer.NewKinesisWriter(session, stats, c)
		if err != nil {
			logger.WithError(err).
				WithField("stream_name", c.StreamName).
				Fatal("Failed to create Kinesis writer")
		}
		writers = append(writers, w)
	}

	return writers
}
