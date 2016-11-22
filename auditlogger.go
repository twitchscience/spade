package main

import (
	"flag"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/twitchscience/aws_utils/logger"
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

func (s *snsHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

func (s *snsHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.topicARN, er)
	if err != nil {
		logger.WithError(err).WithField("sent_error", er).Error("Failed to send error")
	}
}

type nullHarness struct {}

func (n *nullHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

type nullErrorHarness struct {}

func (n *nullErrorHarness) SendError(er error) {}

func newAuditLogger(sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI, replay bool) *gologging.UploadLogger {
	rotateCoordinator := gologging.NewRotateCoordinator(maxLinesPerLog, rotateTime)
	instanceInfo := key_name_generator.BuildInstanceInfo(&key_name_generator.EnvInstanceFetcher{}, "spade_processor_audit", *loggingDir)

	var notifierHarness uploader.NotifierHarness
	var errorHarness uploader.ErrorNotifierHarness
	if replay {
		notifierHarness = &nullHarness{}
		errorHarness = &nullErrorHarness{}
	} else {
		harness := &snsHarness{
			topicARN: config.ProcessorErrorTopicARN,
			notifier: notifier.BuildSNSClient(sns),
		}
		notifierHarness = harness
		errorHarness = harness
	}

	l, err := gologging.StartS3Logger(
		rotateCoordinator,
		instanceInfo,
		notifierHarness,
		uploader.NewFactory(config.AuditBucketName, &key_name_generator.EdgeKeyNameGenerator{Info: instanceInfo}, s3Uploader),
		errorHarness,
		numWorkers,
	)

	if err != nil {
		logger.WithError(err).Fatal("Failed to start audit logger")
	}

	return l
}
