package main

// I need a better name for this file

import (
	"fmt"
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
	"github.com/twitchscience/spade/cache"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/kinesisconfigs"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/reporter"
	tableConfig "github.com/twitchscience/spade/tables"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

const (
	schemaReloadFrequency        = 5 * time.Minute
	schemaRetryDelay             = 2 * time.Second
	kinesisConfigReloadFrequency = 10 * time.Minute
	kinesisConfigRetryDelay      = 2 * time.Second
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

func createValueFetchers(
	jsonFetchers map[string]lookup.JSONValueFetcherConfig,
	stats reporter.StatsLogger,
) map[string]lookup.ValueFetcher {

	allFetchers := map[string]lookup.ValueFetcher{}
	for id, config := range jsonFetchers {
		fetcher, err := lookup.NewJSONValueFetcher(config, stats)
		if err != nil {
			logger.WithError(err).Fatalf("Failed to create a value fetcher with id %s", id)
		}
		allFetchers[id] = fetcher
	}
	return allFetchers
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

func createTransformerCache(s *session.Session, cfg elastimemcache.Config) *elastimemcache.Client {
	tCache, err := elastimemcache.NewClientWithSession(s, cfg)
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

	loader, err := tableConfig.NewDynamicLoader(fetcher, schemaReloadFrequency, schemaRetryDelay,
		stats, tConfigs)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create schema dynamic loader")
	}

	logger.Go(loader.Crank)
	return loader
}

func createKinesisConfigLoader(fetcher fetcher.ConfigFetcher, stats reporter.StatsLogger, multee *writer.Multee, session *session.Session) *kinesisconfigs.DynamicLoader {
	loader, err := kinesisconfigs.NewDynamicLoader(
		fetcher,
		kinesisConfigReloadFrequency,
		kinesisConfigRetryDelay,
		stats,
		multee,
		session)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create kinesis config dynamic loader")
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
	session, err := session.NewSession(&aws.Config{})
	if err != nil {
		logger.WithError(err).Fatal("failed to create geoipupdater")
	}
	u := geoip.NewUpdater(time.Now(), transformer.GeoIPDB, *config, s3.New(session))
	logger.Go(u.UpdateLoop)
	return u
}

// createStaticKinesisWriters creates the writers from JSON configs, which are static
func createStaticKinesisWriters(multee *writer.Multee, session *session.Session, stats statsd.Statter) {
	for _, c := range config.KinesisOutputs {
		w, err := writer.NewKinesisWriter(session, stats, c)
		if err != nil {
			logger.WithError(err).
				WithField("stream_name", c.StreamName).
				Fatal("Failed to create static Kinesis writer")
		}
		multee.Add(fmt.Sprintf("static_%s_%s_%s", c.StreamRole, c.StreamType, c.StreamName), w)
	}
}
