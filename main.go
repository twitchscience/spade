package main

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

func expandGlob(glob []byte) ([]*spade.Event, error) {
	// Hack in just for kick over, test if the array is json
	var e spade.Event
	err := json.Unmarshal(glob, &e)
	if err == nil && e.Version == 3 {
		return []*spade.Event{&e}, nil
	}
	// End hack

	compressed := bytes.NewBuffer(glob)

	v, err := compressed.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading version byte: %s", err)
	}
	if v != compressionVersion {
		return nil, fmt.Errorf("Unknown version, got %v expected %v", v, compressionVersion)
	}

	deflator := flate.NewReader(compressed)
	defer func() {
		_ = deflator.Close()
	}()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, deflator)
	if err != nil {
		return nil, fmt.Errorf("Error decompressiong: %v", err)
	}

	var events []*spade.Event
	err = json.Unmarshal(decompressed.Bytes(), &events)
	if err != nil {
		return nil, fmt.Errorf("Error Unmarhalling: %v", err)
	}

	return events, nil
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

	numGlobs := 0
	numEvents := 0
MainLoop:
	for {
		select {
		case <-sigc:
			break MainLoop
		case <-rotationTicker.C:
			spadeWriter.Rotate()
			s := spadeReporter.Finalize()
			log.Printf("Globs: %d Events %d Stats: %v", numGlobs, numEvents, s)
		case record := <-consumer.C:
			if record.Error != nil {
				log.Printf("Consumer error: %s", record.Error)
			} else {
				numGlobs++
				events, err := expandGlob(record.Data)
				if err == nil && len(events) > 0 {
					uuid := events[0].Uuid
					_, found := duplicateCache.Get(uuid)
					if !found {
						numEvents += len(events)
						for _, e := range events {
							now := time.Now()
							_ = stats.TimingDuration("record.age", now.Sub(e.ReceivedAt), 1.0)
							spadeReporter.IncrementExpected(1)
							d, _ := spade.Marshal(e)
							processorPool.Process(&parseRequest{
								data:  d,
								start: time.Now(),
							})
						}
					} else {
						log.Println("Ignoring duplicate of UUID", uuid)
					}
					duplicateCache.Set(uuid, 0, cache.DefaultExpiration)
				} else if err != nil {
					log.Printf("expandGlob returned error: %v", err)
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
