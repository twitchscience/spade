package log_manager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/geoip"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"
)

const (
	ParserPoolSize      = 10
	TransformerPoolSize = 10
)

type SpadeEdgeLogManager struct {
	Reporter   reporter.Reporter
	Processor  *processor.SpadeProcessorPool
	Writer     writer.SpadeWriter
	Stats      reporter.StatsLogger
	Loader     transformer.ConfigLoader
	Downloader s3manageriface.DownloaderAPI
	GeoUpdater geoip.Updater
}

type EdgeMessage struct {
	Version int
	Keyname string
}

func New(
	outputDir string,
	stats reporter.StatsLogger,
	downloader s3manageriface.DownloaderAPI,
	redshiftUploaderPool *uploader.UploaderPool,
	blueprintUploaderPool *uploader.UploaderPool,
	logger *gologging.UploadLogger,
	fetcher fetcher.ConfigFetcher,
	maxLogBytes int64,
	maxLogAgeSecs int64,
	geoipConfig geoip.Config,
) *SpadeEdgeLogManager {
	reporter := reporter.BuildSpadeReporter(
		&sync.WaitGroup{},
		[]reporter.Tracker{
			&reporter.SpadeStatsdTracker{
				Stats: stats,
			},
			&reporter.SpadeUUIDTracker{
				Logger: logger,
			},
		},
	)
	loader, err := table_config.NewDynamicLoader(
		fetcher,
		5*time.Minute,
		2*time.Second,
		stats,
	)
	if err != nil {
		// If we cant load the config initially we should fail out.
		// Maybe we want a retry mechanism here.
		log.Panicf("Could not load config with fetcher %+v: %v\n", fetcher, err)
	}
	go loader.Crank()

	geoUpdater := geoip.NewUpdater(time.Now(), transformer.GeoIpDB, geoipConfig)
	go geoUpdater.UpdateLoop()

	writerController, err := writer.NewWriterController(
		outputDir,
		reporter,
		redshiftUploaderPool,
		blueprintUploaderPool,
		maxLogBytes,
		maxLogAgeSecs,
	)
	if err != nil {
		log.Panicf("Could not create writer Controller %v\n", err)
	}
	processor := processor.BuildProcessorPool(
		ParserPoolSize,
		TransformerPoolSize,
		loader,
		reporter,
	)
	processor.Listen(writerController)
	return &SpadeEdgeLogManager{
		Reporter:   reporter,
		Writer:     writerController,
		Processor:  processor,
		Loader:     loader,
		Stats:      stats,
		Downloader: downloader,
		GeoUpdater: geoUpdater,
	}
}

// Downloads happen to disk instead of streaming so that if there is a network error
// we abort the handle before any messages are processed. This avoids double processing.
func (s *SpadeEdgeLogManager) Handle(msg *sqs.Message) error {
	err := s.handle(msg)
	return err
}

func (s *SpadeEdgeLogManager) Close() {
	s.Writer.Close()
	s.Processor.Close()
	s.GeoUpdater.Close()
}

func (s *SpadeEdgeLogManager) handle(msg *sqs.Message) error {
	var edgeMessage EdgeMessage

	parser := &LogParser{
		Writer:    s.Writer,
		Reporter:  s.Reporter,
		Processor: s.Processor,
		useGzip:   true,
	}

	err := json.Unmarshal([]byte(aws.StringValue(msg.Body)), &edgeMessage)
	if err != nil {
		return fmt.Errorf("Could not decode %s\n", msg.Body)
	}

	tmpFile, err := ioutil.TempFile("", "spade")
	if err != nil {
		return fmt.Errorf("Failed to create a tempfile to download %s: %v", edgeMessage.Keyname, err)
	}
	defer os.Remove(tmpFile.Name())
	log.Printf("Downloading %s into %s", edgeMessage.Keyname, tmpFile.Name())

	parts := strings.SplitN(edgeMessage.Keyname, "/", 2)
	n, err := s.Downloader.Download(tmpFile, &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})

	if err != nil {
		return fmt.Errorf("Error downloading %s into %s: %v", edgeMessage.Keyname, tmpFile.Name(), err)
	}

	log.Printf("Downloaded a %d byte file\n", n)

	tmpStat, err := tmpFile.Stat()
	if err != nil {
		return fmt.Errorf("Unable to stat %s\n", tmpFile.Name())
	}
	task := &LogTask{
		LogFile:   tmpStat,
		Directory: os.TempDir(),
	}
	return parser.process(task)
}
