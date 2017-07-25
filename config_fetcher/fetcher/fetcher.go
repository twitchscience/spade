package fetcher

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

// ConfigFetcher is responsible for obtaining and/or writing configs from Blueprint.
type ConfigFetcher interface {
	FetchAndWrite(io.ReadCloser, io.WriteCloser) error
	Fetch() (io.ReadCloser, error)
	ConfigDestination(string) (io.WriteCloser, error)
}

// FetchConfig fetches a config and writes it to the given filename.
func FetchConfig(cf ConfigFetcher, outputFileDst string) (err error) {
	src, err := cf.Fetch()
	if err != nil {
		return err
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	dest, err := cf.ConfigDestination(outputFileDst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dest.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return cf.FetchAndWrite(src, dest)
}

type fetcher struct {
	s3        s3iface.S3API
	bucket    string
	key       string
	validator func(b []byte) error
}

// New returns a ConfigFetcher that reads configs from the given URL.
func New(bucket, key string, s3 s3iface.S3API, validator func(b []byte) error) ConfigFetcher {
	return &fetcher{
		s3:        s3,
		bucket:    bucket,
		key:       key,
		validator: validator,
	}
}

// ValidateFetchedSchema validates a fetched schema
func ValidateFetchedSchema(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("There are no bytes to validate schema")
	}
	var cfgs []scoop_protocol.Config
	err := json.Unmarshal(b, &cfgs)
	if err != nil {
		return fmt.Errorf("Result not a valid []schema.Event: %s; error: %s", string(b), err)
	}
	return nil
}

// ValidateFetchedKinesisConfig validates a fetched Kinesis config
func ValidateFetchedKinesisConfig(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("There are no bytes to validate kinesis config")
	}
	var cfgs []scoop_protocol.AnnotatedKinesisConfig
	err := json.Unmarshal(b, &cfgs)
	if err != nil {
		return fmt.Errorf("Result not a valid []writer.AnnotatedKinesisConfig: %s; error: %s", string(b), err)
	}
	return nil
}

// ValidateFetchedEventMetadataConfig validates a fetched event metadata
func ValidateFetchedEventMetadataConfig(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("There are no bytes to validate event metadata config")
	}
	var cfgs scoop_protocol.EventMetadataConfig
	err := json.Unmarshal(b, &cfgs)
	if err != nil {
		return fmt.Errorf("Result not a valid EventMetadataConfig: %s; error: %s", string(b), err)
	}
	return nil
}

// FetchAndWrite reads a config, validates it with the provided validator and writes it out.
func (f *fetcher) FetchAndWrite(src io.ReadCloser, dst io.WriteCloser) error {
	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}

	if err = f.validator(b); err != nil {
		return err
	}

	_, err = dst.Write(b)
	return err
}

// Fetch returns a reader for the config.
func (f *fetcher) Fetch() (io.ReadCloser, error) {
	resp, err := f.s3.GetObject(&s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &f.key,
	})
	if err != nil {
		return nil, err
	}
	gzreader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("creating gzip reader: %v", err)
	}
	return gzreader, nil
}

// ConfigDestination returns a writer to the given path.
func (f *fetcher) ConfigDestination(outputFileName string) (io.WriteCloser, error) {
	return os.Create(outputFileName)
}
