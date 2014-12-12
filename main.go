package main

import (
	"flag"
	"fmt"
	"syscall"

	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/listener"
	"github.com/twitchscience/aws_utils/notifier"
	aws_upload "github.com/twitchscience/aws_utils/uploader"

	"github.com/twitchscience/gologging/gologging"
	gen "github.com/twitchscience/gologging/key_name_generator"

	"github.com/twitchscience/spade/config_fetcher/fetcher"
	"github.com/twitchscience/spade/log_manager"
	_ "github.com/twitchscience/spade/parser/nginx"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/uploader"
	"github.com/twitchscience/spade/writer"

	"github.com/cactus/go-statsd-client/statsd"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"

	"log"
	"os"
	"os/signal"
	"time"
)

const (
	auditBucketName   = "processor-audits"
	MAX_LINES_PER_LOG = 10000000 // 10 million
	ROTATE_TIME       = time.Minute * 10
)

var (
	_dir        = flag.String("spade_dir", ".", "where does spade_log live?")
	logging_dir = flag.String("audit_log_dir", ".", "where does audit_log live?")

	stats_prefix = flag.String("stat_prefix", "edge", "statsd prefix")
	configUrl    = flag.String("config_url", "http://blueprint.twitch.tv/schemas", "the location of blueprint")
	_gzipped     = flag.Bool("gzipped", false, "use to mark if input is gzipped")
	CLOUD_ENV    = environment.GetCloudEnv()
	auditLogger  *gologging.UploadLogger
)

func InitLogger(logDir string) (*os.File, error) {
	file, err := os.Create(
		fmt.Sprintf("%v/%v.%v.log", logDir, "Parser", time.Now().Format("2006-01-02.15:04:00")))
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	return file, nil
}

type DummyNotifierHarness struct{}

// TODO: DRY this up with spade-edge.2014-06-02 16:38
type SQSErrorHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

func (d *DummyNotifierHarness) SendMessage(r *aws_upload.UploadReceipt) error {
	return nil
}

func BuildSQSErrorHarness() *SQSErrorHarness {
	return &SQSErrorHarness{
		qName:    fmt.Sprintf("uploader-error-spade-processor-%s", CLOUD_ENV),
		notifier: notifier.DefaultClient,
	}
}

func (s *SQSErrorHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.qName, er)
	if err != nil {
		log.Println(err)
	}
}

func init() {
	var err error
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalln("Failed to recieve auth from env")
	}
	awsConnection := s3.New(
		auth,
		aws.USWest2,
	)

	auditRotateCoordinator := gologging.NewRotateCoordinator(MAX_LINES_PER_LOG, ROTATE_TIME)

	auditBucket := awsConnection.Bucket(auditBucketName + "-" + CLOUD_ENV)
	auditBucket.PutBucket(s3.BucketOwnerFull)

	auditInfo := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor_audit", *logging_dir)

	auditLogger, err = gologging.StartS3Logger(
		auditRotateCoordinator,
		auditInfo,
		&DummyNotifierHarness{},
		&aws_upload.S3UploaderBuilder{
			Bucket:           auditBucket,
			KeyNameGenerator: &gen.EdgeKeyNameGenerator{Info: auditInfo},
		},
		BuildSQSErrorHarness(),
		2,
	)
	if err != nil {
		log.Fatalf("Got Error while building audit: %s\n", err)
	}
}

func main() {
	flag.Parse()

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalf("Failed to recieve auth from env: %s\n", err)
	}
	awsConnection := s3.New(
		auth,
		aws.USWest2,
	)

	logF, logErr := InitLogger(*_dir + "/spade_logging")
	defer func() {
		if logErr == nil {
			logF.Close()
		}
	}()

	if logErr != nil {
		log.Printf("Unable to open file in %v using for logging STDERR instead\n", *_dir+"/spade_logging")
	}

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

	spadeUploaderPool := uploader.BuildUploaderForRedshift(3, awsConnection)
	blueprintUploaderPool := uploader.BuildUploaderForBlueprint(1, awsConnection)

	sqsListener := listener.BuildSQSListener(&listener.SQSAddr{
		QueueName: "spade-edge-" + CLOUD_ENV,
		Region:    aws.USWest2,
		Auth:      auth,
	}, log_manager.New(
		*_dir,
		reporter.WrapCactusStatter(stats, 0.1),
		awsConnection,
		spadeUploaderPool,
		blueprintUploaderPool,
		auditLogger,
		fetcher.New(*configUrl),
	), time.Second*60)

	wait := make(chan bool)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		sqsListener.Close()
		auditLogger.Close()
		// TODO: rethink the auditlogger logic...
		wait <- true
	}()

	// Start listener
	sqsListener.Listen()
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
