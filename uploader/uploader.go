package uploader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/TwitchScience/aws_utils/environment"
	"github.com/TwitchScience/aws_utils/notifier"
	"github.com/TwitchScience/aws_utils/uploader"

	gen "github.com/TwitchScience/gologging/key_name_generator"

	"github.com/crowdmob/goamz/s3"
)

var (
	CLOUD_ENV = environment.GetCloudEnv()

	uploaderQueue = fmt.Sprintf("spade-compactor-%s", CLOUD_ENV)
	bucketName    = "spade-compacter"
)

type SQSNotifierHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

type ProcessorErrorHandler struct{}

func (p *ProcessorErrorHandler) SendError(err error) {
	log.Println(err)
	e := notifier.DefaultClient.SendMessage("error", "uploader-error-"+uploaderQueue, err)
	if e != nil {
		log.Println(e)
	}
}

func BuildSQSNotifierHarness() *SQSNotifierHarness {
	client := notifier.DefaultClient
	client.Signer.RegisterMessageType("uploadNotify", func(args ...interface{}) (string, error) {
		if len(args) < 2 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"tablename\":\"%s\",\"keyname\":\"%s\"}", args...), nil
	})
	return &SQSNotifierHarness{
		qName:    fmt.Sprintf("spade-compactor-%s", CLOUD_ENV),
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

func BuildUploader(numWorkers int, awsConnection *s3.S3) *uploader.UploaderPool {

	info := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_processor", "")
	bucket := awsConnection.Bucket(bucketName + "-" + CLOUD_ENV)
	bucket.PutBucket(s3.BucketOwnerFull)

	u := uploader.StartUploaderPool(
		numWorkers,
		&ProcessorErrorHandler{},
		BuildSQSNotifierHarness(),
		&uploader.S3UploaderBuilder{
			Bucket:           bucket,
			KeyNameGenerator: &gen.ProcessorKeyNameGenerator{info},
		},
	)
	return u
}
