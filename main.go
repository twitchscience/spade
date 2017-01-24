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
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/patrickmn/go-cache"
	"github.com/twitchscience/aws_utils/logger"
	aws_uploader "github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/spade/cache/elastimemcache"
	"github.com/twitchscience/spade/cache/lru"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/consumer"
	"github.com/twitchscience/spade/deglobber"
	"github.com/twitchscience/spade/geoip"
	jsonLog "github.com/twitchscience/spade/parser/json"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
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
)

var (
	_dir        = flag.String("spade_dir", ".", "where does spade_log live?")
	statsPrefix = flag.String("stat_prefix", "processor", "statsd prefix")
	replay      = flag.Bool("replay", false, "take plaintext events (as in spade-edge-prod) from standard input")
	runTag      = flag.String("run_tag", "", "override YYYYMMDD tag with new prefix (usually for replay)")
)

func init() {
	if err := jsonLog.Register(os.Getenv("REJECT_ON_BAD_FIRST_IP") != ""); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup jsonLog parser: %v\n", err)
		os.Exit(1)
	}
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
	tCache                *elastimemcache.Client

	rotation <-chan time.Time
	sigc     chan os.Signal
}

func newProcessor() *spadeProcessor {
	// aws resources
	session := session.New(&aws.Config{
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

	sns := sns.New(session)
	s3Uploader := s3manager.NewUploader(session)

	stats := createStatsdStatter()
	startELBHealthCheckListener()

	reporterStats := reporter.WrapCactusStatter(stats, 0.01)
	spadeReporter := createSpadeReporter(reporterStats)
	valueFetchers := createValueFetchers(config.JSONValueFetchers, reporterStats)
	spadeUploaderPool := uploader.BuildUploaderForRedshift(
		redshiftUploaderNumWorkers, sns, s3Uploader, config.AceBucketName,
		config.AceTopicARN, config.AceErrorTopicARN, *runTag, *replay)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(
		blueprintUploaderNumWorkers, sns, s3Uploader, config.NonTrackedBucketName,
		config.NonTrackedTopicARN, config.NonTrackedErrorTopicARN, *replay)

	multee := &writer.Multee{}
	multee.Add(createSpadeWriter(
		*_dir, spadeReporter, spadeUploaderPool, blueprintUploaderPool,
		config.MaxLogBytes, config.MaxLogAgeSecs))
	multee.AddMany(createKinesisWriters(session, stats))

	fetcher := fetcher.New(config.BlueprintSchemasURL)
	localCache := lru.New(1000)
	remoteCache := createTransformerCache(session, config.TransformerCacheCluster)
	tConfigs := createMappingTransformerConfigs(
		valueFetchers, localCache, remoteCache, config.TransformerFetchers, reporterStats)
	schemaLoader := createSchemaLoader(fetcher, reporterStats, tConfigs)

	processorPool := processor.BuildProcessorPool(schemaLoader, spadeReporter, multee, reporterStats)
	processorPool.StartListeners()

	deglobberPool := deglobber.NewPool(deglobber.PoolConfig{
		ProcessorPool:      processorPool,
		Stats:              stats,
		DuplicateCache:     cache.New(duplicateCacheExpiry, duplicateCacheCleanupFrequency),
		PoolSize:           runtime.NumCPU(),
		CompressionVersion: compressionVersion,
		ReplayMode:         *replay,
	})
	deglobberPool.Start()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	return &spadeProcessor{
		geoIPUpdater:          createGeoipUpdater(config.Geoip),
		spadeReporter:         spadeReporter,
		resultPipe:            createPipe(session, stats, *replay),
		deglobberPool:         deglobberPool,
		processorPool:         processorPool,
		multee:                multee,
		spadeUploaderPool:     spadeUploaderPool,
		blueprintUploaderPool: blueprintUploaderPool,
		rotation:              time.Tick(rotationCheckFrequency),
		sigc:                  sigc,
		tCache:                remoteCache,
	}
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
	s.tCache.StopAutoDiscovery()

	s.resultPipe.Close()
	s.deglobberPool.Close()
	s.processorPool.Close()
	if err := s.multee.Close(); err != nil {
		logger.WithError(err).Error("multee.Close() failed")
	}
	s.geoIPUpdater.Close()

	err := uploader.ClearEventsFolder(s.spadeUploaderPool, *_dir+"/"+writer.EventsDir+"/")
	if err != nil {
		logger.WithError(err).Error("Failed to clear events directory")
	}

	err = uploader.ClearEventsFolder(s.blueprintUploaderPool, *_dir+"/"+writer.NonTrackedDir+"/")
	if err != nil {
		logger.WithError(err).Error("Failed to clear untracked events directory")
	}

	s.spadeUploaderPool.Close()
	s.blueprintUploaderPool.Close()

}

func main() {
	flag.Parse()
	loadConfig()
	logger.InitWithRollbar("info", config.RollbarToken, config.RollbarEnvironment)
	logger.Info("Starting processor")
	logger.CaptureDefault()
	defer logger.LogPanic()

	// Start listener for pprof.
	logger.Go(func() {
		defaultErr := http.ListenAndServe(net.JoinHostPort("", "8081"), http.DefaultServeMux)
		logger.WithError(defaultErr).Error("Error listening to port 8081 with http.DefaultServeMux")
	})

	s := newProcessor()
	s.run()

	logger.Info("Main loop exited, shutting down")
	s.shutdown()

	logger.Info("Exiting main cleanly.")
	logger.Wait()
}
