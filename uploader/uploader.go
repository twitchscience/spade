package uploader

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
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
	version, err := extractEventVersion(message.Path)
	if err != nil {
		return fmt.Errorf("extracting event version from path: %v", err)
	}
	return s.notifier.SendMessage("uploadNotify", s.topicARN, extractEventName(message.Path), message.KeyName, version)
}

// NullNotifierHarness is a stub SNS client that does nothing.  In replay mode, we don't need to notify anyone of the
// newly uploaded files.
type NullNotifierHarness struct{}

// SendMessage is a noop
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

func extractEventVersion(filename string) (int, error) {
	path := strings.LastIndex(filename, ".v") + 2
	ext := strings.Index(filename, ".gz")
	if ext < 0 {
		ext = len(filename)
	}
	return strconv.Atoi(filename[path:ext])
}

// ProcessorErrorHandler sends messages about errors sending SNS messages to another topic.
type ProcessorErrorHandler struct {
	topicARN string
	notifier *notifier.SNSClient
}

// SendError sends the sending error to an topic.
func (p *ProcessorErrorHandler) SendError(err error) {
	logger.WithError(err).Error("error sending message to topic")
	e := p.notifier.SendMessage("error", p.topicARN, err)
	if e != nil {
		logger.WithError(e).Error("failed to send error")
	}
}

// NullErrorHandler logs errors but does not send an SNS message.
type NullErrorHandler struct{}

// SendError logs the given error.
func (n *NullErrorHandler) SendError(err error) {
	logger.WithError(err).Error("")
}

func buildErrorHandler(sns snsiface.SNSAPI, errorTopicArn string, nullNotifier bool) uploader.ErrorNotifierHarness {
	if nullNotifier {
		return &NullErrorHandler{}
	}
	return &ProcessorErrorHandler{
		notifier: notifier.BuildSNSClient(sns),
		topicARN: errorTopicArn,
	}
}

// remove the file or log the error and move on
func removeOrLog(path string) {
	err := os.Remove(path)
	if err != nil {
		logger.WithError(err).WithField("path", path).Error("failed to remove file")
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
		removeOrLog(path)
	}
}

// salvageData uses the utility gzrecover to recover partial data from gzip,
// overwriting the corrupted gzip file. Returns false if no data was recovered
// or there was an error, else true
func salvageData(path string) (bool, error) {
	cmd := exec.Command("gzrecover", "-p", path)
	var salvaged bytes.Buffer
	cmd.Stdout = &salvaged
	err := cmd.Run()
	if err != nil {
		return false, fmt.Errorf("running gzrecover: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return false, fmt.Errorf("creating file to overwrite with salvaged data: %v", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			logger.WithField("path", path).WithError(cerr).Error("failed to close salvaged file")
		}
	}()

	writer := gzip.NewWriter(f)
	defer func() {
		if cerr := writer.Close(); cerr != nil {
			logger.WithField("path", path).WithError(cerr).Error("failed to close salvaged data gzip writer")
		}
	}()

	// Writes (via gzip to file) all lines of the gzrecover output but the last,
	// as the last line likely was only partially written.
	writeSuccess := false
	for {
		bytes, err := salvaged.ReadBytes('\n')
		if err == io.EOF {
			return writeSuccess, nil
		} else if err != nil {
			return false, fmt.Errorf("reading from gzrecover output: %v", err)
		}

		if _, err = writer.Write(bytes); err != nil {
			return false, fmt.Errorf("writing salvaged data: %v", err)
		}
		writeSuccess = true
	}

}

func isValidGzip(path string) bool {
	entry := logger.WithField("path", path)
	file, err := os.Open(path)
	if err != nil {
		entry.WithError(err).Error("failed to open")
		return false
	}
	defer func() {
		if err = file.Close(); err != nil {
			entry.WithError(err).Error("failed to close file")
		}
	}()

	reader, err := gzip.NewReader(file)
	if err != nil {
		entry.WithError(err).Error("failed to create gzip.NewReader")
		return false
	}
	defer func() {
		if err = reader.Close(); err != nil {
			entry.WithError(err).Error("failed to close reader")
		}
	}()

	_, err = ioutil.ReadAll(reader)
	if err != nil {
		entry.WithError(err).Error("failed to read gzipped file")
		return false
	}

	return true
}

func walkEventFiles(eventsDir string, f func(path string)) error {
	return filepath.Walk(eventsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != eventsDir {
			return filepath.SkipDir
		}
		if strings.HasSuffix(path, ".gz") {
			f(path)
		}
		return nil
	})
}

// ClearEventsFolder uploads all files in the eventsDir.
func ClearEventsFolder(uploaderPool *uploader.UploaderPool, eventsDir string) error {
	return walkEventFiles(eventsDir, func(path string) {
		SafeGzipUpload(uploaderPool, path)
	})
}

// SalvageCorruptedEvents salvages in place all invalid gzip files in the eventsDir
func SalvageCorruptedEvents(eventsDir string) error {
	return walkEventFiles(eventsDir, func(path string) {
		if !isValidGzip(path) {
			salvaged, err := salvageData(path)
			if !salvaged {
				removeOrLog(path)
			}
			if err != nil {
				logger.WithField("path", path).WithError(err).Error("salvaging data from corrupted gzip file")
			}
		}
	})
}

func buildInstanceInfo(replay bool) *gen.InstanceInfo {
	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")
	if replay {
		info.Node = os.Getenv("HOST")
	}
	return info
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
		buildErrorHandler(input.sns, input.errorTopicARN, input.nullNotifier),
		buildNotifierHarness(input.sns, input.topicARN, input.nullNotifier),
		uploader.NewFactory(input.bucketName, input.keyNameGenerator, input.s3Uploader),
	)
}

func redshiftKeyNameGenerator(info *gen.InstanceInfo, runTag string, replay bool) uploader.S3KeyNameGenerator {
	if replay {
		return &gen.ReplayKeyNameGenerator{Info: info, RunTag: runTag}
	}
	return &gen.ProcessorKeyNameGenerator{Info: info}
}

// BuildUploaderForRedshift builds an Uploader that uploads files to s3 and notifies sns.
func BuildUploaderForRedshift(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	aceBucketName, aceTopicARN, aceErrorTopicARN, runTag string, replay bool) *uploader.UploaderPool {

	return buildUploader(&buildUploaderInput{
		bucketName:       aceBucketName,
		topicARN:         aceTopicARN,
		errorTopicARN:    aceErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: redshiftKeyNameGenerator(buildInstanceInfo(replay), runTag, replay),
		nullNotifier:     replay,
	})
}

// BuildUploaderForBlueprint builds an Uploader that uploads non-tracked events to s3 and notifies sns.
func BuildUploaderForBlueprint(numWorkers int, sns snsiface.SNSAPI, s3Uploader s3manageriface.UploaderAPI,
	nonTrackedBucketName, nonTrackedTopicARN, nonTrackedErrorTopicARN string, replay bool) *uploader.UploaderPool {

	return buildUploader(&buildUploaderInput{
		bucketName:       nonTrackedBucketName,
		topicARN:         nonTrackedTopicARN,
		errorTopicARN:    nonTrackedErrorTopicARN,
		numWorkers:       numWorkers,
		sns:              sns,
		s3Uploader:       s3Uploader,
		keyNameGenerator: &gen.EdgeKeyNameGenerator{Info: buildInstanceInfo(replay)},
		nullNotifier:     replay,
	})
}
