package main

// I need a better name for this file

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/cache"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	eventMetadataConfig "github.com/twitchscience/spade/event_metadata"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/kinesisconfigs"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/reporter"
	tableConfig "github.com/twitchscience/spade/tables"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
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

func createTransformerCache(ec elasticacheiface.ElastiCacheAPI, cfg elastimemcache.Config) *elastimemcache.Client {
	tCache, err := elastimemcache.NewClientWithInterface(ec, cfg)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create transformer cache")
	}
	logger.Go(tCache.StartAutoDiscovery)
	return tCache
}

func createMappingTransformerConfigs(
	valueFetchers map[string]lookup.ValueFetcher,
	localCache cache.StringCache,
	remoteCache cache.StringCache,
	fetchersMapping map[string]string,
	stats reporter.StatsLogger,
) map[string]transformer.MappingTransformerConfig {

	tConfigs := map[string]transformer.MappingTransformerConfig{}
	for tID, fID := range fetchersMapping {
		fetcher, ok := valueFetchers[fID]
		if !ok {
			logger.Fatalf("Failed to find a value fetcher with id %s", fID)
		}
		tConfigs[tID] = transformer.MappingTransformerConfig{
			Fetcher:     fetcher,
			LocalCache:  localCache,
			RemoteCache: remoteCache,
			Stats:       stats,
		}
	}
	return tConfigs
}

func createSchemaLoader(
	fetcher fetcher.ConfigFetcher,
	stats reporter.StatsLogger,
	tConfigs map[string]transformer.MappingTransformerConfig,
) *tableConfig.DynamicLoader {

	loader, err := tableConfig.NewDynamicLoader(fetcher, config.SchemaReloadFrequency.Duration,
		config.SchemaRetryDelay.Duration, stats, tConfigs)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create schema dynamic loader")
	}

	logger.Go(loader.Crank)
	return loader
}

func createKinesisConfigLoader(fetcher fetcher.ConfigFetcher, stats reporter.StatsLogger, multee writer.SpadeWriterManager, kinesisFactory writer.KinesisFactory, firehoseFactory writer.FirehoseFactory) *kinesisconfigs.DynamicLoader {
	loader, err := kinesisconfigs.NewDynamicLoader(
		fetcher,
		config.KinesisConfigReloadFrequency.Duration,
		config.KinesisConfigRetryDelay.Duration,
		stats,
		multee,
		func(cfg scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error) {
			return writer.NewKinesisWriter(
				kinesisFactory,
				firehoseFactory,
				stats.GetStatter(),
				cfg,
				config.KinesisWriterErrorsBeforeThrottling,
				config.KinesisWriterErrorThrottlePeriodSeconds)
		},
		config.KinesisOutputs,
	)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create kinesis config dynamic loader")
	}

	logger.Go(loader.Crank)
	return loader
}

func createEventMetadataLoader(fetcher fetcher.ConfigFetcher, stats reporter.StatsLogger) *eventMetadataConfig.DynamicLoader {
	loader, err := eventMetadataConfig.NewDynamicLoader(fetcher, config.EventMetadataReloadFrequency.Duration,
		config.EventMetadataRetryDelay.Duration, stats)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create event metadata dynamic loader")
	}

	logger.Go(loader.Crank)
	return loader
}

func createPipe(kinesis kinesisiface.KinesisAPI, dynamodb dynamodbiface.DynamoDBAPI, stats statsd.Statter, replay bool) consumer.ResultPipe {
	if replay {
		return consumer.NewStandardInputPipe()
	}

	consumer, err := consumer.NewKinesisPipe(kinesis, dynamodb, stats, config.Consumer)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create consumer")
	}
	return consumer
}

func createGeoipUpdater(s3 s3iface.S3API, config *geoip.Config) *geoip.Updater {
	u := geoip.NewUpdater(time.Now(), transformer.GeoIPDB, *config, s3)
	logger.Go(u.UpdateLoop)
	return u
}

// createStaticKinesisWriters creates the writers from JSON configs, which are static
func createStaticKinesisWriters(multee *writer.Multee, kinesisFactory writer.KinesisFactory, firehoseFactory writer.FirehoseFactory, stats statsd.Statter) {
	for _, c := range config.KinesisOutputs {
		w, err := writer.NewKinesisWriter(
			kinesisFactory,
			firehoseFactory,
			stats,
			c,
			config.KinesisWriterErrorsBeforeThrottling,
			config.KinesisWriterErrorThrottlePeriodSeconds)
		if err != nil {
			logger.WithError(err).
				WithField("stream_name", c.StreamName).
				Fatal("Failed to create static Kinesis writer")
		}
		multee.Add(fmt.Sprintf("static_%s_%s_%s", c.StreamRole, c.StreamType, c.StreamName), w)
	}
}
