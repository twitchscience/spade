package uploader

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	gen "github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// SNSNotifierHarness is an SNS client that writes messages about uploaded files to a specific ARN.
type SNSNotifierHarness struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendMessage sends information to SNS about file uploaded to S3.
func (s *SNSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("uploadNotify", s.topicARN, extractEventName(message.Path), message.KeyName, extractEventVersion(message.Path))
}

// NullNotifierHarness is a stub SNS client that does nothing.  In replay mode, we don't need to notify anyone of the
// newly uploaded files.
type NullNotifierHarness struct{}

func (n *NullNotifierHarness) SendMessage(_ *uploader.UploadReceipt) error {
	return nil
}

func buildNotifierHarness(sns snsiface.SNSAPI, topicARN string, replay bool) uploader.NotifierHarness {
	if replay {
		return &NullNotifierHarness{}
	}

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
	return &SNSNotifierHarness{topicARN: topicARN, notifier: client}
}

func extractEventName(filename string) string {
	path := strings.LastIndex(filename, "/") + 1
	ext := path + strings.Index(filename[path:], ".")
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

// ProcessorErrorHandler sends messages about errors sending SNS messages to another topic.
type ProcessorErrorHandler struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendError sends the sending error to an topic.
func (p *ProcessorErrorHandler) SendError(err error) {
	logger.WithError(err).Error("")
	e := p.notifier.SendMessage("error", p.topicARN, err)
	if e != nil {
		logger.WithError(e).Error("Failed to send error")
	}
}

// SafeGzipUpload validates a file is a valid gzip file and then uploads it.
func SafeGzipUpload(uploaderPool *uploader.UploaderPool, path string) {
	if isValidGzip(path) {
		uploaderPool.Upload(&uploader.UploadRequest{
			Filename: path,
			FileType: uploader.Gzip,
		})
	} else {
		logger.WithField("path", path).Warn("Given path is not a valid gzip file; removing")
		err := os.Remove(path)
		if err != nil {
			logger.WithError(err).WithField("path", path).Error("Failed to remove path")
		}
	}
}

func isValidGzip(path string) bool {
	entry := logger.WithField("path", path)
	file, err := os.Open(path)
	if err != nil {
		entry.WithError(err).Error("Failed to open")
		return false
	}
	defer func() {
		if err = file.Close(); err != nil {
			entry.WithError(err).Error("Failed to close file")
		}
	}()

	reader, err := gzip.NewReader(file)
	if err != nil {
		entry.WithError(err).Error("Failed to create gzip.NewReader")
		return false
	}
	defer func() {
		if err = reader.Close(); err != nil {
			entry.WithError(err).Error("Failed to close reader")
		}
	}()

	_, err = ioutil.ReadAll(reader)
	if err != nil {
		entry.WithError(err).Error("Failed to read gzipped file")
		return false
	}

	return true
}

// ClearEventsFolder uploads all files in the eventsDir.
func ClearEventsFolder(uploaderPool *uploader.UploaderPool, eventsDir string) error {
	return filepath.Walk(eventsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != eventsDir {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, ".gz") {
			SafeGzipUpload(uploaderPool, path)
		}
		return nil
	})
}

type buildUploaderInput struct {
	bucketName       string
	topicARN         string
	errorTopicARN    string
	numWorkers       int
	sns              snsiface.SNSAPI
	s3Uploader       s3manageriface.UploaderAPI
	keyNameGenerator uploader.S3KeyNameGenerator
	nullNotifier     bool
}

func buildUploader(input *buildUploaderInput) *uploader.UploaderPool {
	return uploader.StartUploaderPool(
		input.numWorkers,
		&ProcessorErrorHandler{
			notifier: notifier.BuildSNSClient(input.sns),
			topicARN: input.errorTopicARN,
		},
		buildNotifierHarness(input.sns, input.topicARN, input.nullNotifier),
		uploader.NewFactory(input.bucketName, input.keyNameGenerator, input.s3Uploader),
	)
}

func redshiftKeyNameGenerator(info *gen.InstanceInfo, runTag string, replay bool) uploader.S3KeyNameGenerator {
	if replay {
		return &gen.ReplayKeyNameGenerator{Info: info, RunTag: runTag}
	} else {
		return &gen.ProcessorKeyNameGenerator{Info: info}
	}
}

// BuildUploaderForRedshift builds an Uploader that uploads files to s3 and notifies sns.
func BuildUploaderForRedshift(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	aceBucketName, aceTopicARN, aceErrorTopicARN, runTag string, replay bool) *uploader.UploaderPool {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")

	return buildUploader(&buildUploaderInput{
		bucketName:       aceBucketName,
		topicARN:         aceTopicARN,
		errorTopicARN:    aceErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: redshiftKeyNameGenerator(info, runTag, replay),
		nullNotifier:     replay,
	})
}

// BuildUploaderForBlueprint builds an Uploader that uploads non-tracked events to s3 and notifies sns.
func BuildUploaderForBlueprint(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	nonTrackedBucketName, nonTrackedTopicARN, nonTrackedErrorTopicARN string) *uploader.UploaderPool {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")

	return buildUploader(&buildUploaderInput{
		bucketName:       nonTrackedBucketName,
		topicARN:         nonTrackedTopicARN,
		errorTopicARN:    nonTrackedErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: &gen.EdgeKeyNameGenerator{Info: info},
	})
}
