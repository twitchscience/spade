package uploader

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	gen "github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

type SNSNotifierHarness struct {
	topicARN string
	notifier *notifier.SNSClient
}

type ProcessorErrorHandler struct {
	topicARN string
	notifier *notifier.SNSClient
}

func (p *ProcessorErrorHandler) SendError(err error) {
	log.Println(err)
	e := p.notifier.SendMessage("error", p.topicARN, err)
	if e != nil {
		log.Println(e)
	}
}

func BuildSNSNotifierHarness(sns snsiface.SNSAPI, topicARN string) *SNSNotifierHarness {
	client := notifier.BuildSNSClient(sns)
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
	return &SNSNotifierHarness{
		topicARN: topicARN,
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

func (s *SNSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("uploadNotify", s.topicARN, extractEventName(message.Path), message.KeyName, extractEventVersion(message.Path))
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

type buildUploaderInput struct {
	bucketName    string
	topicARN      string
	errorTopicARN string
	numWorkers    int
	sns           snsiface.SNSAPI
	s3Uploader    s3manageriface.UploaderAPI
}

func buildUploader(input *buildUploaderInput) *uploader.UploaderPool {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")

	return uploader.StartUploaderPool(
		input.numWorkers,
		&ProcessorErrorHandler{
			notifier: notifier.BuildSNSClient(input.sns),
			topicARN: input.errorTopicARN,
		},
		BuildSNSNotifierHarness(input.sns, input.topicARN),
		uploader.NewFactory(input.bucketName, &gen.ProcessorKeyNameGenerator{Info: info}, input.s3Uploader),
	)
}

func BuildUploaderForRedshift(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI, aceBucketName, aceTopicARN, aceErrorTopicARN string) *uploader.UploaderPool {
	return buildUploader(&buildUploaderInput{
		bucketName:    aceBucketName,
		topicARN:      aceTopicARN,
		errorTopicARN: aceErrorTopicARN,
		numWorkers:    numWorkers,
		sns:           sns,
		s3Uploader:    s3Uploader,
	})
}

func BuildUploaderForBlueprint(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI, nonTrackedBucketName, nonTrackedTopicARN, nonTrackedErrorTopicARN string) *uploader.UploaderPool {
	return buildUploader(&buildUploaderInput{
		bucketName:    nonTrackedBucketName,
		topicARN:      nonTrackedTopicARN,
		errorTopicARN: nonTrackedErrorTopicARN,
		numWorkers:    numWorkers,
		sns:           sns,
		s3Uploader:    s3Uploader,
	})
}
