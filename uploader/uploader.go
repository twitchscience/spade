package uploader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"

	gen "github.com/twitchscience/gologging/key_name_generator"

	"github.com/crowdmob/goamz/s3"
)

var (
	CLOUD_ENV = environment.GetCloudEnv()

	redshiftQueue      = fmt.Sprintf("spade-compactor-%s", CLOUD_ENV)
	redshiftBucketName = "spade-compacter"

	nonTrackedQueue      = fmt.Sprintf("spade-nontracked-%s", CLOUD_ENV)
	nonTrackedBucketName = "spade-nontracked"
)

type SQSNotifierHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

type ProcessorErrorHandler struct {
	qName string
}

func (p *ProcessorErrorHandler) SendError(err error) {
	log.Println(err)
	e := notifier.DefaultClient.SendMessage("error", "uploader-error-"+p.qName, err)
	if e != nil {
		log.Println(e)
	}
}

func BuildSQSNotifierHarness(qName string) *SQSNotifierHarness {
	client := notifier.DefaultClient
	client.Signer.RegisterMessageType("uploadNotify", func(args ...interface{}) (string, error) {
		if len(args) < 2 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"tablename\":\"%s\",\"keyname\":\"%s\"}", args...), nil
	})
	return &SQSNotifierHarness{
		qName:    qName,
		notifier: client,
	}
}

func extractEventName(filename string) string {
	path := strings.LastIndex(filename, "/") + 1
	ext := strings.Index(filename, ".")
	if ext < 0 {
		ext = len(filename)
	}
	return filename[path:ext]
}

func (s *SQSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("uploadNotify", s.qName, extractEventName(message.Path), message.KeyName)
}

func ClearEventsFolder(uploaderPool *uploader.UploaderPool, eventsDir string) error {
	return filepath.Walk(eventsDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && path != eventsDir {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, ".gz") {
			uploaderPool.Upload(&uploader.UploadRequest{
				Filename: path,
				FileType: uploader.Gzip,
			})
		}
		return nil
	})
}

func buildUploader(bucketName, queueName string, numWorkers int, awsConnection *s3.S3) *uploader.UploaderPool {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")
	bucket := awsConnection.Bucket(bucketName + "-" + CLOUD_ENV)
	bucket.PutBucket(s3.BucketOwnerFull)

	return uploader.StartUploaderPool(
		numWorkers,
		&ProcessorErrorHandler{qName: queueName},
		BuildSQSNotifierHarness(queueName),
		&uploader.S3UploaderBuilder{
			Bucket:           bucket,
			KeyNameGenerator: &gen.ProcessorKeyNameGenerator{Info: info},
		},
	)
}

func BuildUploaderForRedshift(numWorkers int, awsConnection *s3.S3) *uploader.UploaderPool {
	return buildUploader(redshiftBucketName, redshiftQueue, numWorkers, awsConnection)
}

func BuildUploaderForBlueprint(numWorkers int, awsConnection *s3.S3) *uploader.UploaderPool {
	return buildUploader(nonTrackedBucketName, nonTrackedQueue, numWorkers, awsConnection)
}
