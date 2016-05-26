package main

import (
	"flag"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/patrickmn/go-cache"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	jsonLog "github.com/twitchscience/spade/parser/json_log"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/uploader"
	"github.com/twitchscience/spade/writer"

	"log"
	"os"
	"os/signal"

	"github.com/cactus/go-statsd-client/statsd"
)

const (
	redshiftUploaderNumWorkers     = 3
	blueprintUploaderNumWorkers    = 1
	rotationCheckFrequency         = 2 * time.Second
	duplicateCacheExpiry           = 5 * time.Minute
	duplicateCacheCleanupFrequency = 1 * time.Minute
)

var (
	_dir        = flag.String("spade_dir", ".", "where does spade_log live?")
	statsPrefix = flag.String("stat_prefix", "processor", "statsd prefix")
)

func init() {
	jsonLog.Register(os.Getenv("REJECT_ON_BAD_FIRST_IP") != "")
}

type parseRequest struct {
	data  []byte
	start time.Time
}

func (p *parseRequest) Data() []byte {
	return p.data
}

func (p *parseRequest) StartTime() time.Time {
	return p.start
}

func main() {
	flag.Parse()
	loadConfig()

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
			log.Fatalf("Statsd configuration error: %v", err)
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}

	// Listener for ELB health check
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		err := http.ListenAndServe(net.JoinHostPort("", "8080"), healthMux)
		if err != nil {
			log.Printf("Error listening to port 8080 with healthMux %v\n", err)
		}
	}()

	geoIpUpdater := createGeoipUpdater(config.Geoip)
	auditLogger := newAuditLogger(sns, s3Uploader)
	reporterStats := reporter.WrapCactusStatter(stats, 0.1)
	spadeReporter := createSpadeReporter(reporterStats, auditLogger)
	spadeUploaderPool := uploader.BuildUploaderForRedshift(redshiftUploaderNumWorkers, sns, s3Uploader, config.AceBucketName, config.AceTopicARN, config.AceErrorTopicARN)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(blueprintUploaderNumWorkers, sns, s3Uploader, config.NonTrackedBucketName, config.NonTrackedTopicARN, config.NonTrackedErrorTopicARN)
	spadeWriter := createSpadeWriter(*_dir, spadeReporter, spadeUploaderPool, blueprintUploaderPool, config.MaxLogBytes, config.MaxLogAgeSecs)
	fetcher := fetcher.New(config.BlueprintSchemasURL)
	schemaLoader := createSchemaLoader(fetcher, reporterStats)
	processorPool := createProcessorPool(schemaLoader, spadeReporter)
	processorPool.Listen(spadeWriter)
	consumer := createConsumer(session, stats)
	rotationTicker := time.NewTicker(rotationCheckFrequency)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	duplicateCache := cache.New(duplicateCacheExpiry, duplicateCacheCleanupFrequency)

	numProcessed := 0
MainLoop:
	for {
		select {
		case <-sigc:
			break MainLoop
		case <-rotationTicker.C:
			spadeWriter.Rotate()
			s := spadeReporter.Finalize()
			log.Printf("Processed: %d Stats: %v", numProcessed, s)
		case record := <-consumer.C:
			if record.Error != nil {
				log.Printf("Consumer error: %s", record.Error)
			} else {
				numProcessed++
				var rawEvent spade.Event
				err := spade.Unmarshal(record.Data, &rawEvent)
				if err == nil {
					_, found := duplicateCache.Get(rawEvent.Uuid)
					if !found {
						now := time.Now()
						_ = stats.TimingDuration("record.age", now.Sub(rawEvent.ReceivedAt), 1.0)
						spadeReporter.IncrementExpected(1)
						processorPool.Process(&parseRequest{
							data:  record.Data,
							start: time.Now(),
						})
					} else {
						log.Println("Ignoring duplicate of UUID", rawEvent.Uuid)
					}
					duplicateCache.Set(rawEvent.Uuid, 0, cache.DefaultExpiration)
				}
			}
		}
	}

	consumer.Close()
	spadeWriter.Close()
	processorPool.Close()
	geoIpUpdater.Close()
	auditLogger.Close()

	err := uploader.ClearEventsFolder(spadeUploaderPool, *_dir+"/"+writer.EventsDir+"/")
	if err != nil {
		log.Println(err)
	}

	err = uploader.ClearEventsFolder(blueprintUploaderPool, *_dir+"/"+writer.NonTrackedDir+"/")
	if err != nil {
		log.Println(err)
	}

	spadeUploaderPool.Close()

	blueprintUploaderPool.Close()
}
