package fetcher

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// ConfigFetcher is responsible for obtaining and/or writing configs from Blueprint.
type ConfigFetcher interface {
	Fetch() (io.ReadCloser, error)
}

type fetcher struct {
	s3     s3iface.S3API
	bucket string
	key    string
}

// New returns a ConfigFetcher that reads configs from the given s3 bucket+key.
func New(bucket, key string, s3 s3iface.S3API) ConfigFetcher {
	return &fetcher{
		s3:     s3,
		bucket: bucket,
		key:    key,
	}
}

// Fetch returns a reader for the config.
func (f *fetcher) Fetch() (io.ReadCloser, error) {
	resp, err := f.s3.GetObject(&s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &f.key,
	})
	if err != nil {
		return nil, fmt.Errorf("retrieving config object from s3: %v", err)
	}
	gzreader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("creating gzip reader: %v", err)
	}
	return gzreader, nil
}
