package uploader

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	gen "github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
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
	qName    string
	notifier *notifier.SQSClient
}

func (p *ProcessorErrorHandler) SendError(err error) {
	log.Println(err)
	e := p.notifier.SendMessage("error", "uploader-error-"+p.qName, err)
	if e != nil {
		log.Println(e)
	}
}

func BuildSQSNotifierHarness(sqs sqsiface.SQSAPI, qName string) *SQSNotifierHarness {
	client := notifier.BuildSQSClient(sqs)
	client.Signer.RegisterMessageType("uploadNotify", func(args ...interface{}) (string, error) {
		if len(args) != 3 {
			return "", errors.New("Missing correct number of args")
		}
		message := scoop_protocol.RowCopyRequest{
			TableName:    args[0].(string),
			KeyName:      args[1].(string),
			TableVersion: args[2].(int),
		}

		jsonMessage, err := json.Marshal(message)
		if err != nil {
			return "", err
		}
		return string(jsonMessage), nil
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

func extractEventVersion(filename string) int {
	path := strings.LastIndex(filename, ".v") + 2
	ext := strings.Index(filename, ".gz")
	if ext < 0 {
		ext = len(filename)
	}
	val, _ := strconv.Atoi(filename[path:ext])
	return val
}

func (s *SQSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("uploadNotify", s.qName, extractEventName(message.Path), message.KeyName, extractEventVersion(message.Path))
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

func buildUploader(bucketName, queueName string, numWorkers int, sqs sqsiface.SQSAPI, s3Uploader s3manageriface.UploaderAPI) *uploader.UploaderPool {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")
	bucket := bucketName + "-" + CLOUD_ENV

	return uploader.StartUploaderPool(
		numWorkers,
		&ProcessorErrorHandler{
			notifier: notifier.BuildSQSClient(sqs),
			qName:    queueName,
		},
		BuildSQSNotifierHarness(sqs, queueName),
		uploader.NewFactory(bucket, &gen.ProcessorKeyNameGenerator{Info: info}, s3Uploader),
	)
}

func BuildUploaderForRedshift(numWorkers int, sqs sqsiface.SQSAPI, s3Uploader s3manageriface.UploaderAPI) *uploader.UploaderPool {
	return buildUploader(redshiftBucketName, redshiftQueue, numWorkers, sqs, s3Uploader)
}

func BuildUploaderForBlueprint(numWorkers int, sqs sqsiface.SQSAPI, s3Uploader s3manageriface.UploaderAPI) *uploader.UploaderPool {
	return buildUploader(nonTrackedBucketName, nonTrackedQueue, numWorkers, sqs, s3Uploader)
}
