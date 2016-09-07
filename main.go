package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/patrickmn/go-cache"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/deglobber"
	jsonLog "github.com/twitchscience/spade/parser/json"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/uploader"
	"github.com/twitchscience/spade/writer"

	"os"
	"os/signal"

	"github.com/cactus/go-statsd-client/statsd"
)

const (
	redshiftUploaderNumWorkers          = 3
	blueprintUploaderNumWorkers         = 1
	rotationCheckFrequency              = 2 * time.Second
	duplicateCacheExpiry                = 5 * time.Minute
	duplicateCacheCleanupFrequency      = 1 * time.Minute
	compressionVersion             byte = 1
)

var (
	_dir        = flag.String("spade_dir", ".", "where does spade_log live?")
	statsPrefix = flag.String("stat_prefix", "processor", "statsd prefix")
	replay      = flag.Bool("replay", false, "take plaintext events (as in spade-edge-prod) from standard input")
)

func init() {
	if err := jsonLog.Register(os.Getenv("REJECT_ON_BAD_FIRST_IP") != ""); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup jsonLog parser: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	loadConfig()
	logger.InitWithRollbar("info", config.RollbarToken, config.RollbarEnvironment)
	logger.Info("Starting processor")
	logger.CaptureDefault()
	defer logger.LogPanic()

	// aws resources
	session := session.New(&aws.Config{
		HTTPClient: &http.Client{
			Timeout: 6200 * time.Millisecond,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   6200 * time.Millisecond,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: 6200 * time.Millisecond,
				MaxIdleConnsPerHost: 100,
			},
		},
	})

	sns := sns.New(session)
	s3Uploader := s3manager.NewUploader(session)

	// Set up statsd monitoring
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	var stats statsd.Statter
	if statsdHostport == "" {
		stats, _ = statsd.NewNoop()
	} else {
		var err error
		if stats, err = statsd.New(statsdHostport, *statsPrefix); err != nil {
			logger.WithError(err).Fatal("Statsd configuration error")
		}
		logger.WithField("statsd_host_port", statsdHostport).Info("Connected to statsd")
	}

	// Listener for ELB health check
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	logger.Go(func() {
		err := http.ListenAndServe(net.JoinHostPort("", "8080"), healthMux)
		if err != nil {
			logger.WithError(err).Error("Failure listening to port 8080 with healthMux")
		}
	})

	geoIPUpdater := createGeoipUpdater(config.Geoip)
	auditLogger := newAuditLogger(sns, s3Uploader)
	reporterStats := reporter.WrapCactusStatter(stats, 0.1)
	spadeReporter := createSpadeReporter(reporterStats, auditLogger)
	spadeUploaderPool := uploader.BuildUploaderForRedshift(redshiftUploaderNumWorkers, sns, s3Uploader, config.AceBucketName, config.AceTopicARN, config.AceErrorTopicARN)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(blueprintUploaderNumWorkers, sns, s3Uploader, config.NonTrackedBucketName, config.NonTrackedTopicARN, config.NonTrackedErrorTopicARN)

	multee := &writer.Multee{}
	spadeWriter := createSpadeWriter(*_dir, spadeReporter, spadeUploaderPool, blueprintUploaderPool, config.MaxLogBytes, config.MaxLogAgeSecs)
	kinesisWriters := createKinesisWriters(session, stats)
	multee.Add(spadeWriter)
	multee.AddMany(kinesisWriters)

	fetcher := fetcher.New(config.BlueprintSchemasURL)
	schemaLoader := createSchemaLoader(fetcher, reporterStats)

	processorPool := processor.BuildProcessorPool(schemaLoader, spadeReporter, multee)
	processorPool.StartListeners()
	duplicateCache := cache.New(duplicateCacheExpiry, duplicateCacheCleanupFrequency)
	deglobberPool := deglobber.NewPool(deglobber.PoolConfig{
		ProcessorPool:      processorPool,
		Stats:              stats,
		DuplicateCache:     duplicateCache,
		PoolSize:           runtime.NumCPU(),
		CompressionVersion: compressionVersion,
		ReplayMode:         *replay,
	})
	deglobberPool.Start()

	resultPipe := createPipe(session, stats, *replay)

	rotation := time.Tick(rotationCheckFrequency)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)

	numGlobs := 0
MainLoop:
	for {
		select {
		case <-sigc:
			break MainLoop
		case <-rotation:
			if _, err := multee.Rotate(); err != nil {
				logger.WithError(err).Error("multee.Rotate() failed")
				break MainLoop
			}
			logger.WithFields(map[string]interface{}{
				"num_globs": numGlobs,
				"stats":     spadeReporter.Report(),
			}).Info("Processed data rotated to output")
		case record, ok := <-resultPipe.ReadChannel():
			if !ok {
				logger.Info("Read channel closed")
				break MainLoop
			}

			if record.Error != nil {
				logger.WithError(record.Error).Error("Consumer failed")
				break MainLoop
			}

			numGlobs++
			deglobberPool.Submit(record.Data)
		}
	}

	resultPipe.Close()
	deglobberPool.Close()
	processorPool.Close()
	if err := multee.Close(); err != nil {
		logger.WithError(err).Error("multee.Close() failed")
	}
	geoIPUpdater.Close()
	auditLogger.Close()

	err := uploader.ClearEventsFolder(spadeUploaderPool, *_dir+"/"+writer.EventsDir+"/")
	if err != nil {
		logger.WithError(err).Error("Failed to clear events directory")
	}

	err = uploader.ClearEventsFolder(blueprintUploaderPool, *_dir+"/"+writer.NonTrackedDir+"/")
	if err != nil {
		logger.WithError(err).Error("Failed to clear untracked events directory")
	}

	spadeUploaderPool.Close()

	blueprintUploaderPool.Close()
	logger.Wait()
}
