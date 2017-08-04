package geoip

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/twitchscience/aws_utils/logger"
)

// Updater keeps a GeoLookup's databases updated.
type Updater struct {
	lastUpdated time.Time
	closer      chan bool
	geo         GeoLookup
	config      Config
	s3          s3iface.S3API
}

// Keypath is the combination of the s3 key to read from and the local path to write to
type Keypath struct {
	Key  string
	Path string
}

// Config defines how a GeoLookup should be kept updated.
type Config struct {
	ConfigBucket        string
	IPCity              Keypath
	IPASN               Keypath
	UpdateFrequencyMins int
	JitterSecs          int
}

// NewUpdater returns a new Updater for the given GeoLookup.
func NewUpdater(lastUpdated time.Time, geo GeoLookup, config Config, s3 s3iface.S3API) *Updater {
	return &Updater{
		lastUpdated: lastUpdated,
		closer:      make(chan bool),
		geo:         geo,
		config:      config,
		s3:          s3,
	}
}

func writeWithRename(data io.Reader, fname string) error {
	buf := new(bytes.Buffer)
	partName := fname + ".part"
	if _, err := buf.ReadFrom(data); err != nil {
		return fmt.Errorf("error reading data for '%s': %s", partName, err)
	}
	if err := ioutil.WriteFile(partName, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("error writing object to file '%s': %s", partName, err)
	}
	if err := os.Rename(partName, fname); err != nil {
		return fmt.Errorf("error renaming file '%s' to '%s': %s", partName, fname, err)
	}
	return nil
}

// getIfNew downloads the new geoip dbs from s3 if there are new ones, and returns
// a boolean true if it was new
func (u *Updater) getIfNew() (bool, error) {
	for _, kp := range []Keypath{u.config.IPCity, u.config.IPASN} {
		resp, err := u.s3.GetObject(&s3.GetObjectInput{
			Bucket:          aws.String(u.config.ConfigBucket),
			Key:             aws.String(kp.Key),
			IfModifiedSince: aws.Time(u.lastUpdated),
		})
		if err != nil {
			if err, ok := err.(awserr.RequestFailure); ok && err.StatusCode() == 304 {
				// Not a new geoip db
				return false, nil
			}
			return false, fmt.Errorf("Error getting s3 object at 's3://%s/%s': %s", u.config.ConfigBucket, kp.Key, err)
		}
		err = writeWithRename(resp.Body, kp.Path)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// UpdateLoop is a blocking function that updates the GeoLookup on a recurring basis.
func (u *Updater) UpdateLoop() {
	tick := time.NewTicker(time.Duration(u.config.UpdateFrequencyMins) * time.Minute)
	for {
		select {
		case <-tick.C:
			jitter := time.Duration(rand.Intn(u.config.JitterSecs)) * time.Second
			time.Sleep(jitter)
			logger.Info("Pulling down new GeoIP DBs if they are new")
			newDB, err := u.getIfNew()
			if err != nil {
				logger.WithError(err).Error("Failed to get the new GeoIP")
				continue
			}
			if newDB {
				if err := u.geo.Reload(); err != nil {
					logger.WithError(err).Error("Error reloading Geo DB")
					continue
				}
				u.lastUpdated = time.Now()
				logger.Info("Loaded and using new GeoIP DBs")
			} else {
				logger.WithField("update_period", u.config.UpdateFrequencyMins).
					Info("GeoIP DBs are not new, waiting to try again")
			}

		case <-u.closer:
			return
		}
	}
}

// Close stops the UpdateLoop for the Updater.
func (u *Updater) Close() {
	u.closer <- true
}
