package main

import (
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/gologging/key_name_generator"
)

const (
	maxLinesPerLog = 10000000 // 10 million
	rotateTime     = time.Minute * 10
	numWorkers     = 2
)

var (
	loggingDir = flag.String("audit_log_dir", ".", "where does audit_log live?")
)

type snsHarness struct {
	topicARN string
	notifier *notifier.SNSClient
}

func (d *snsHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

func (s *snsHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.topicARN, er)
	if err != nil {
		log.Println(err)
	}
}

func newAuditLogger(sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI) *gologging.UploadLogger {
	c := gologging.NewRotateCoordinator(maxLinesPerLog, rotateTime)
	i := key_name_generator.BuildInstanceInfo(&key_name_generator.EnvInstanceFetcher{}, "spade_processor_audit", *loggingDir)
	h := &snsHarness{
		topicARN: config.ProcessorErrorTopicARN,
		notifier: notifier.BuildSNSClient(sns),
	}
	l, err := gologging.StartS3Logger(
		c,
		i,
		h,
		uploader.NewFactory(config.AuditBucketName, &key_name_generator.EdgeKeyNameGenerator{Info: i}, s3Uploader),
		h,
		numWorkers,
	)

	if err != nil {
		log.Fatalf("Error starting audit logger: %s", err)
	}

	return l
}
