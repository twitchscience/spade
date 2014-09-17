package uploader

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/crowdmob/goamz/s3"

	"github.com/twitchscience/aws_utils/common"
)

type FileTypeHeader string

const (
	Gzip FileTypeHeader = "application/x-gzip"
	Text FileTypeHeader = "text/plain"
)

func TimeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(rwTimeout))
		return conn, nil
	}
}

func NewTimeoutClient(connectTimeout time.Duration, readWriteTimeout time.Duration) *http.Client {
	http.DefaultClient.Transport = &http.Transport{
		Dial: TimeoutDialer(connectTimeout, readWriteTimeout),
	}
	return http.DefaultClient
}

type Uploader interface {
	GetKeyName(string) string
	Upload(*UploadRequest) (*UploadReceipt, error)
	TargetLocation() string
}

type S3UploaderBuilder struct {
	Bucket           *s3.Bucket
	KeyNameGenerator S3KeyNameGenerator
}

type S3Uploader struct {
	Bucket           *s3.Bucket
	KeyNameGenerator S3KeyNameGenerator
}

func (builder *S3UploaderBuilder) BuildUploader() Uploader {
	return &S3Uploader{
		Bucket:           builder.Bucket,
		KeyNameGenerator: builder.KeyNameGenerator,
	}
}

func (worker *S3Uploader) prependBucketName(path string) string {
	return fmt.Sprintf("%s/%s", worker.Bucket.Name, path)
}

func (worker *S3Uploader) TargetLocation() string {
	return worker.Bucket.Name
}

func (worker *S3Uploader) GetKeyName(filename string) string {
	return worker.KeyNameGenerator.GetKeyName(filename)
}

var retrier = &common.Retrier{
	Times:         3,
	BackoffFactor: 2,
}

func (worker *S3Uploader) Upload(req *UploadRequest) (*UploadReceipt, error) {
	file, err := os.Open(req.Filename)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// This means that if we fail to talk to s3 we still remove the file.
	// I think that this is the correct behavior as we dont want to cause
	// a HD overflow in case of a http timeout.
	defer os.Remove(req.Filename)
	keyName := worker.GetKeyName(req.Filename)

	err = retrier.Retry(func() error {
		// We need to seek to ensure that the retries read from the start of the file
		file.Seek(0, 0)
		return worker.Bucket.PutReader(
			keyName,
			file,
			info.Size(),
			string(req.FileType),
			"bucket-owner-full-control",
			s3.Options{},
		)
	})
	if err != nil {
		return nil, err
	}
	return &UploadReceipt{
		Path:    req.Filename,
		KeyName: worker.prependBucketName(keyName),
	}, nil
}
