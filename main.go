/*
Package spade provides a parallel processing layer for event data in the
Spade pipeline. It consumes data written by the Spade Edge, processes it
according to rules in Blueprint, and writes it to Kinesis streams and TSV
files intended for Redshift (via rs_ingester). It expects events to be a
base64 encoded JSON object or list of objects with an event field and a
properties field. It rejects any data it doesn’t recognize or cannot decode,
including unmapped properties of correctly formatted data. Decodable but
unmapped event types are flushed to S3 periodically so that Blueprint can
suggest schemas for them.
*/
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cactus/go-statsd-client/statsd"
	cache "github.com/patrickmn/go-cache"
	"github.com/twitchscience/aws_utils/logger"
	aws_uploader "github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/cache/lru"
	"github.com/twitchscience/spade/config"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/deglobber"
	eventMetadataConfig "github.com/twitchscience/spade/event_metadata"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/kinesisconfigs"
	"github.com/twitchscience/spade/lookup"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
	tableConfig "github.com/twitchscience/spade/tables"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/uploader"
	"github.com/twitchscience/spade/writer"
)

const (
	redshiftUploaderNumWorkers          = 6
	blueprintUploaderNumWorkers         = 1
	rotationCheckFrequency              = 2 * time.Second
	duplicateCacheExpiry                = 5 * time.Minute
	duplicateCacheCleanupFrequency      = 1 * time.Minute
	networkTimeout                      = 6200 * time.Millisecond
	compressionVersion             byte = 1
	spadeWriterKey                      = "SpadeWriter"
)

var (
	_replay         = flag.Bool("replay", false, "take plaintext events (as in spade-edge-prod) from standard input")
	_runTag         = flag.String("run_tag", "", "override YYYYMMDD tag with new prefix (usually for replay)")
	_configFilename = flag.String("config", "conf.json", "name of config file")
)

type closer interface {
	Close()
}

type spadeProcessor struct {
	geoIPUpdater  *geoip.Updater
	spadeReporter reporter.Reporter

	resultPipe            consumer.ResultPipe
	deglobberPool         *deglobber.Pool
	processorPool         processor.Pool
	multee                *writer.Multee
	spadeUploaderPool     *aws_uploader.UploaderPool
	blueprintUploaderPool *aws_uploader.UploaderPool
	closers               []closer

	rotation <-chan time.Time
	sigc     chan os.Signal
}

type spadeProcessorDeps struct {
	s3              s3iface.S3API
	s3Uploader      s3manageriface.UploaderAPI
	sns             snsiface.SNSAPI
	elasticache     elasticacheiface.ElastiCacheAPI
	kinesisFactory  writer.KinesisFactory
	firehoseFactory writer.FirehoseFactory

	valueFetcherFactory func(lookup.JSONValueFetcherConfig, reporter.StatsLogger) (lookup.ValueFetcher, error)
	resultPipe          consumer.ResultPipe
	memcacheClient      elastimemcache.MemcacheClient
	memcacheSelector    elastimemcache.ServerList
	geoip               geoip.GeoLookup
	stats               statsd.Statter
	cfg                 *config.Config
	runTag              string
	replay              bool
}

func buildDeps(cfg *config.Config, runTag string, replay bool) (*spadeProcessorDeps, error) {
	// aws resources
	session, err := session.NewSession(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: networkTimeout,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   networkTimeout,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: networkTimeout,
				MaxIdleConnsPerHost: 100,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating aws session: %v", err)
	}

	geoip, err := geoip.NewGeoMMIp(cfg.Geoip.IPCity.Path, cfg.Geoip.IPASN.Path)
	if err != nil {
		return nil, fmt.Errorf("creating geoip db: %v", err)
	}
	statsd, err := createStatsdStatter(cfg.StatsdHostport, cfg.StatsdPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating statsd statter: %v", err)
	}
	ss := &memcache.ServerList{}

	var resultPipe consumer.ResultPipe
	if replay {
		resultPipe = consumer.NewStandardInputPipe()
	} else {
		resultPipe, err = consumer.NewKinesisPipe(
			kinesis.New(session), dynamodb.New(session), statsd, cfg.Consumer)
		if err != nil {
			return nil, fmt.Errorf("creating consumer: %v", err)
		}
	}

	s3 := s3.New(session)
	return &spadeProcessorDeps{
		s3:                  s3,
		s3Uploader:          s3manager.NewUploaderWithClient(s3),
		sns:                 sns.New(session),
		elasticache:         elasticache.New(session),
		kinesisFactory:      &writer.DefaultKinesisFactory{Session: session},
		firehoseFactory:     &writer.DefaultFirehoseFactory{Session: session},
		valueFetcherFactory: lookup.NewJSONValueFetcher,
		resultPipe:          resultPipe,
		geoip:               geoip,
		memcacheClient:      memcache.NewFromSelector(ss),
		memcacheSelector:    ss,
		stats:               statsd,
		cfg:                 cfg,
		runTag:              runTag,
		replay:              replay,
	}, nil
}

func newProcessor(deps *spadeProcessorDeps) (*spadeProcessor, error) {

	reporterStats := reporter.WrapCactusStatter(deps.stats, 0.01)
	spadeReporter := reporter.BuildSpadeReporter(
		[]reporter.Tracker{&reporter.SpadeStatsdTracker{Stats: reporterStats}})

	spadeUploaderPool := uploader.BuildUploaderForRedshift(
		redshiftUploaderNumWorkers, deps.sns, deps.s3Uploader, deps.cfg.AceBucketName,
		deps.cfg.AceTopicARN, deps.cfg.AceErrorTopicARN, deps.runTag, deps.replay)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(
		blueprintUploaderNumWorkers, deps.sns, deps.s3Uploader, deps.cfg.NonTrackedBucketName,
		deps.cfg.NonTrackedTopicARN, deps.cfg.NonTrackedErrorTopicARN, deps.replay)

	err := initializeDirectories(deps.cfg.SpadeDir+"/"+writer.EventsDir+"/",
		deps.cfg.SpadeDir+"/"+writer.NonTrackedDir+"/", spadeUploaderPool)
	if err != nil {
		return nil, fmt.Errorf("initializing directories: %v", err)
	}

	multee := writer.NewMultee()
	spadeWriter := writer.NewWriterController(deps.cfg.SpadeDir, spadeReporter,
		spadeUploaderPool, blueprintUploaderPool,
		deps.cfg.MaxLogBytes, deps.cfg.MaxLogAgeSecs, deps.cfg.NontrackedMaxLogAgeSecs)
	multee.Add(spadeWriterKey, spadeWriter)

	for _, c := range deps.cfg.KinesisOutputs {
		w, werr := writer.NewKinesisWriter(
			deps.kinesisFactory,
			deps.firehoseFactory,
			deps.stats,
			c,
			deps.cfg.KinesisWriterErrorsBeforeThrottling,
			deps.cfg.KinesisWriterErrorThrottlePeriodSeconds)
		if werr != nil {
			return nil, fmt.Errorf("creating static kinesis writer %s: %v", c.StreamName, werr)
		}
		multee.Add(fmt.Sprintf("static_%s_%s_%s", c.StreamRole, c.StreamType, c.StreamName), w)
	}

	processorPool, closers, err := startProcessorPool(deps, multee, spadeReporter, reporterStats)
	if err != nil {
		return nil, fmt.Errorf("starting processor pool: %v", err)
	}

	deglobberPool := deglobber.NewPool(deglobber.PoolConfig{
		ProcessorPool:      processorPool,
		Stats:              deps.stats,
		DuplicateCache:     cache.New(duplicateCacheExpiry, duplicateCacheCleanupFrequency),
		PoolSize:           runtime.NumCPU(),
		CompressionVersion: compressionVersion,
		ReplayMode:         deps.replay,
	})
	deglobberPool.Start()

	gip := geoip.NewUpdater(time.Now(), deps.geoip, *deps.cfg.Geoip, deps.s3)
	logger.Go(gip.UpdateLoop)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	return &spadeProcessor{
		geoIPUpdater:          gip,
		spadeReporter:         spadeReporter,
		resultPipe:            deps.resultPipe,
		deglobberPool:         deglobberPool,
		processorPool:         processorPool,
		multee:                multee,
		spadeUploaderPool:     spadeUploaderPool,
		blueprintUploaderPool: blueprintUploaderPool,
		rotation:              time.Tick(rotationCheckFrequency),
		sigc:                  sigc,
		closers:               closers,
	}, nil
}

func startProcessorPool(deps *spadeProcessorDeps, multee *writer.Multee,
	spadeReporter reporter.Reporter, reporterStats reporter.StatsLogger) (processor.Pool, []closer, error) {
	schemaFetcher := fetcher.New(deps.cfg.ConfigBucket, deps.cfg.SchemasKey, deps.s3)
	kinesisConfigFetcher := fetcher.New(deps.cfg.ConfigBucket, deps.cfg.KinesisConfigKey, deps.s3)
	eventMetadataFetcher := fetcher.New(deps.cfg.ConfigBucket, deps.cfg.MetadataConfigKey, deps.s3)
	localCache := lru.New(1000, time.Duration(deps.cfg.LRULifetimeSeconds)*time.Second)
	remoteCache, err := elastimemcache.NewClientWithInterface(
		deps.elasticache, deps.memcacheClient, deps.memcacheSelector, deps.cfg.TransformerCacheCluster)
	if err != nil {
		return nil, nil, fmt.Errorf("creating transformer cache: %v", err)
	}
	logger.Go(remoteCache.StartAutoDiscovery)

	valueFetchers := map[string]lookup.ValueFetcher{}
	for id, cfg := range deps.cfg.JSONValueFetchers {
		fetcher, fErr := deps.valueFetcherFactory(cfg, reporterStats)
		if fErr != nil {
			return nil, nil, fmt.Errorf("creating value fetcher with id %s: %v", id, fErr)
		}
		valueFetchers[id] = fetcher
	}

	tConfigs := map[string]transformer.MappingTransformerConfig{}
	for tID, fID := range deps.cfg.TransformerFetchers {
		fetcher, ok := valueFetchers[fID]
		if !ok {
			return nil, nil, fmt.Errorf("finding value fetcher with id %s", fID)
		}
		tConfigs[tID] = transformer.MappingTransformerConfig{
			Fetcher:     fetcher,
			LocalCache:  localCache,
			RemoteCache: remoteCache,
			Stats:       reporterStats,
		}
	}

	schemaLoader, err := tableConfig.NewDynamicLoader(
		schemaFetcher, deps.cfg.SchemaReloadFrequency.Duration,
		deps.cfg.SchemaRetryDelay.Duration, reporterStats, tConfigs, deps.geoip)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic schema loader: %v", err)
	}
	logger.Go(schemaLoader.Crank)

	kinesisConfigLoader, err := kinesisconfigs.NewDynamicLoader(
		kinesisConfigFetcher,
		deps.cfg.KinesisConfigReloadFrequency.Duration,
		deps.cfg.KinesisConfigRetryDelay.Duration,
		reporterStats,
		multee,
		func(cfg scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error) {
			return writer.NewKinesisWriter(
				deps.kinesisFactory,
				deps.firehoseFactory,
				reporterStats.GetStatter(),
				cfg,
				deps.cfg.KinesisWriterErrorsBeforeThrottling,
				deps.cfg.KinesisWriterErrorThrottlePeriodSeconds)
		},
		deps.cfg.KinesisOutputs,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic kinesis config loader: %v", err)
	}
	logger.Go(kinesisConfigLoader.Crank)

	eventMetadataLoader, err := eventMetadataConfig.NewDynamicLoader(
		eventMetadataFetcher, deps.cfg.EventMetadataReloadFrequency.Duration,
		deps.cfg.EventMetadataRetryDelay.Duration, reporterStats)
	if err != nil {
		return nil, nil, fmt.Errorf("creating dynamic event metadata loader: %v", err)
	}
	logger.Go(eventMetadataLoader.Crank)

	processorPool := processor.BuildProcessorPool(schemaLoader, eventMetadataLoader, spadeReporter, multee, reporterStats)
	processorPool.StartListeners()
	return processorPool, []closer{
		schemaLoader, kinesisConfigLoader, eventMetadataLoader, remoteCache}, nil
}

func initializeDirectories(events, nontracked string, uploaderPool *aws_uploader.UploaderPool) error {
	// Create event and nontracked directories if they don't exist.
	if _, err := os.Stat(events); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(events, 0755)
		if err != nil {
			return fmt.Errorf("creating event dir: %v", err)
		}
	}
	if _, err := os.Stat(nontracked); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(nontracked, 0755)
		if err != nil {
			return fmt.Errorf("creating nontracked dir: %v", err)
		}
	}
	// If the processor exited hard, clean up the files that were left.
	if err := uploader.SalvageCorruptedEvents(events); err != nil {
		return fmt.Errorf("salvaging corrupted events: %v", err)
	}
	if err := uploader.ClearEventsFolder(uploaderPool, events); err != nil {
		return fmt.Errorf("clearing events folder: %v", err)
	}
	return nil
}

func createStatsdStatter(hostport, prefix string) (statsd.Statter, error) {
	// - If the env is not set up we wil use a noop connection
	if hostport == "" {
		logger.Warning("statsd hostport is empty, using noop statsd connection")
		stats, err := statsd.NewNoop()
		if err != nil {
			return nil, fmt.Errorf("creating noop statsd connection: %v", err)
		}
		return stats, nil
	}

	stats, err := statsd.New(hostport, prefix)
	if err != nil {
		return nil, fmt.Errorf("creating real statsd connection: %v", err)
	}
	logger.WithField("statsd_host_port", hostport).Info("Connected to statsd")
	return stats, nil
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
func (s *spadeProcessor) run() {
	numGlobs := 0
	for {
		select {
		case <-s.sigc:
			logger.Info("Sigint received -- shutting down")
			return
		case <-s.rotation:
			if _, err := s.multee.Rotate(); err != nil {
				logger.WithError(err).Error("multee.Rotate() failed")
				return
			}
			logger.WithFields(map[string]interface{}{
				"num_globs": numGlobs,
				"stats":     s.spadeReporter.Report(),
			}).Info("Processed data rotated to output")
		case record, ok := <-s.resultPipe.ReadChannel():
			if !ok {
				logger.Info("Read channel closed")
				return
			} else if err := record.Error; err != nil {
				logger.WithError(err).Error("Consumer failed")
				return
			} else {
				s.deglobberPool.Submit(record.Data)
				numGlobs++
			}
		}
	}
}

func (s *spadeProcessor) shutdown() {
	var wg sync.WaitGroup
	for _, closer := range s.closers {
		c := closer
		wg.Add(1)
		logger.Go(func() {
			c.Close()
			wg.Done()
		})
	}
	signal.Stop(s.sigc)

	s.resultPipe.Close()
	s.deglobberPool.Close()
	s.processorPool.Close()
	if err := s.multee.Close(); err != nil {
		logger.WithError(err).Error("multee.Close() failed")
	}
	s.geoIPUpdater.Close()

	s.spadeUploaderPool.Close()
	s.blueprintUploaderPool.Close()
	wg.Wait()
	logger.WithFields(map[string]interface{}{
		"stats": s.spadeReporter.Report(),
	}).Info("Processed data rotated to output")
}

func main() {
	flag.Parse()
	cfg, err := config.LoadConfig(*_configFilename, *_replay)
	if err != nil {
		logger.WithField("configFilename", *_configFilename).WithError(
			err).Error("Failed to load config")
		logger.Wait()
		os.Exit(1)
	}
	logger.InitWithRollbar("info", cfg.RollbarToken, cfg.RollbarEnvironment)
	logger.Info("Starting processor")
	logger.CaptureDefault()
	defer logger.LogPanic()

	// Start listener for pprof.
	logger.Go(func() {
		logger.WithError(http.ListenAndServe(":7766", http.DefaultServeMux)).
			Error("Serving pprof failed")
	})

	deps, err := buildDeps(cfg, *_runTag, *_replay)
	if err != nil {
		logger.WithError(err).Error("Failed to build deps")
		logger.Wait()
		os.Exit(1)
	}
	s, err := newProcessor(deps)
	if err != nil {
		logger.WithError(err).Error("Failed to build processor")
		logger.Wait()
		os.Exit(1)
	}

	startELBHealthCheckListener()

	s.run()

	logger.Info("Main loop exited, shutting down")
	s.shutdown()

	logger.Info("Exiting main cleanly.")
	logger.Wait()
}
