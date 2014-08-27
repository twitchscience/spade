package log_manager

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"

	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"

	"github.com/crowdmob/goamz/s3"
	"github.com/crowdmob/goamz/sqs"
)

var env = environment.GetCloudEnv()

const (
	ParserPoolSize      = 10
	TransformerPoolSize = 10
)

type SpadeEdgeLogManager struct {
	Reporter  reporter.Reporter
	Processor *processor.SpadeProcessorPool
	Writer    writer.SpadeWriter
	Stats     reporter.StatsLogger
	Loader    transformer.ConfigLoader

	S3 *s3.S3
}

type EdgeMessage struct {
	Version int
	Keyname string
}

func New(
	outputDir string,
	stats reporter.StatsLogger,
	s3 *s3.S3,
	spadeUploaderPool *uploader.UploaderPool,
	logger *gologging.UploadLogger,
	fetcher fetcher.ConfigFetcher,
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

	writerController := writer.NewWriterController(outputDir, reporter, spadeUploaderPool)
	processor := processor.BuildProcessorPool(
		ParserPoolSize,
		TransformerPoolSize,
		loader,
		reporter,
	)
	processor.Listen(writerController)
	return &SpadeEdgeLogManager{
		Reporter:  reporter,
		Writer:    writerController,
		Processor: processor,
		Loader:    loader,
		Stats:     stats,
		S3:        s3,
	}
}

// Downloads happen to disk instead of streaming so that if there is a network error
// we abort the handle before any messages are processed. This avoids double processing.
func (s *SpadeEdgeLogManager) Handle(msg *sqs.Message) error {
	err := s.handle(msg)

	// Only run the heap dump on test boxes
	if env == "test" {
		runtime.GC()
		now := time.Now()
		f, e := os.Create(fmt.Sprintf("Profile.%s.prof", now.Format("2006-01-02.15-04-05")))
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return err
		}
		e = pprof.WriteHeapProfile(f)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return err
		}
	}
	return err
}

func (s *SpadeEdgeLogManager) handle(msg *sqs.Message) error {
	var edgeMessage EdgeMessage

	parser := &LogParser{
		Writer:    s.Writer,
		Reporter:  s.Reporter,
		Processor: s.Processor,
		useGzip:   true,
	}

	err := json.Unmarshal([]byte(msg.Body), &edgeMessage)
	if err != nil {
		return fmt.Errorf("Could not decode %s\n", msg.Body)
	}
	parts := strings.SplitN(edgeMessage.Keyname, "/", 2)
	bucket := s.S3.Bucket(parts[0])

	readCloser, err := bucket.GetReader(parts[1])
	if err != nil {
		return fmt.Errorf("Unable to get s3 reader %s on bucket %s with key %s\n",
			err, "spade-edge-"+env, edgeMessage.Keyname)
	}
	defer readCloser.Close()

	b := make([]byte, 8)
	rand.Read(b)

	tmpFile, err := ioutil.TempFile("", fmt.Sprintf("%08x", b))
	log.Println("using ", edgeMessage.Keyname)
	if err != nil {
		return fmt.Errorf("cloud not open temp file for %s as %s:  %s\n",
			edgeMessage.Keyname, fmt.Sprintf("%08x", b), err)
	}
	defer os.Remove(tmpFile.Name())
	writer := bufio.NewWriter(tmpFile)
	n, err := writer.ReadFrom(readCloser)
	if err != nil {
		return fmt.Errorf("Encounted err while downloading %s: %s\n", edgeMessage.Keyname, err)
	}
	writer.Flush()
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
