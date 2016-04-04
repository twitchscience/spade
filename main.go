package main

import (
	"flag"
	"fmt"
	"syscall"

	gen "github.com/twitchscience/gologging/key_name_generator"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/aws_utils/notifier"
	aws_upload "github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/log_manager"
	jsonLog "github.com/twitchscience/spade/parser/json_log"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/uploader"
	"github.com/twitchscience/spade/writer"

	"log"
	"os"
	"os/signal"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

const (
	auditBucketName = "processor-audits"
	MaxLinesPerLog  = 10000000 // 10 million
	RotateTime      = time.Minute * 10
)

var (
	_dir        = flag.String("spade_dir", ".", "where does spade_log live?")
	logging_dir = flag.String("audit_log_dir", ".", "where does audit_log live?")

	sqsPollInterval = flag.Duration("sqs_poll_interval", 60*time.Second, "how often should we poll SQS?")
	stats_prefix    = flag.String("stat_prefix", "processor", "statsd prefix")
	configUrl       = flag.String("config_url", "http://blueprint.twitch.tv/schemas", "the location of blueprint")
	s3ConfigPrefix  = flag.String("s3_config_prefix", "", "S3 key to the config directory, with trailing slash")
	CLOUD_ENV       = environment.GetCloudEnv()
	auditLogger     *gologging.UploadLogger
)

type DummyNotifierHarness struct{}

// TODO: DRY this up with spade-edge.2014-06-02 16:38
type SQSErrorHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

func (d *DummyNotifierHarness) SendMessage(r *aws_upload.UploadReceipt) error {
	return nil
}

func BuildSQSErrorHarness(sqs sqsiface.SQSAPI) *SQSErrorHarness {
	return &SQSErrorHarness{
		qName:    fmt.Sprintf("uploader-error-spade-processor-%s", CLOUD_ENV),
		notifier: notifier.BuildSQSClient(sqs),
	}
}

func (s *SQSErrorHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.qName, er)
	if err != nil {
		log.Println(err)
	}
}

func init() {
	jsonLog.Register(os.Getenv("REJECT_ON_BAD_FIRST_IP") != "")
}

func main() {
	flag.Parse()

	session := session.New()
	sqs := sqs.New(session)
	s3Uploader := s3manager.NewUploader(session)
	s3Downloader := s3manager.NewDownloader(session)

	var err error

	// Set up statsd monitoring
	// - If the env is not set up we wil use a noop connection
	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	var stats statsd.Statter
	if statsdHostport == "" {
		stats, _ = statsd.NewNoop()
	} else {
		if stats, err = statsd.New(statsdHostport, *stats_prefix); err != nil {
			log.Fatalf("Statsd configuration error: %v", err)
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}

	auditRotateCoordinator := gologging.NewRotateCoordinator(MaxLinesPerLog, RotateTime)

	auditBucket := auditBucketName + "-" + CLOUD_ENV
	auditInfo := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor_audit", *logging_dir)

	auditLogger, err = gologging.StartS3Logger(
		auditRotateCoordinator,
		auditInfo,
		&DummyNotifierHarness{},
		aws_upload.NewFactory(auditBucket, &gen.EdgeKeyNameGenerator{Info: auditInfo}, s3Uploader),
		BuildSQSErrorHarness(sqs),
		2,
	)
	if err != nil {
		log.Fatalf("Got Error while building audit: %s\n", err)
	}

	spadeUploaderPool := uploader.BuildUploaderForRedshift(3, sqs, s3Uploader)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(1, sqs, s3Uploader)

	lm := log_manager.New(
		*_dir,
		reporter.WrapCactusStatter(stats, 0.1),
		s3Downloader,
		spadeUploaderPool,
		blueprintUploaderPool,
		auditLogger,
		fetcher.New(*configUrl),
		*s3ConfigPrefix,
	)

	sqsListener := listener.BuildSQSListener(lm, *sqsPollInterval, sqs)

	wait := make(chan bool)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		sqsListener.Close()
		auditLogger.Close()
		lm.Close()
		// TODO: rethink the auditlogger logic...
		wait <- true
	}()

	// Start listener
	sqsListener.Listen("spade-edge-" + CLOUD_ENV)
	<-wait

	err = uploader.ClearEventsFolder(spadeUploaderPool, *_dir+"/"+writer.EventsDir+"/")
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
