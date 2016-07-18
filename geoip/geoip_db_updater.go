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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/twitchscience/aws_utils/logger"
)

type Updater struct {
	lastUpdated time.Time
	closer      chan bool
	geo         GeoLookup
	config      Config
	ipCityPath  string
	ipASNPath   string
}

type Config struct {
	ConfigBucket        string
	IpCityKey           string
	IpASNKey            string
	UpdateFrequencyMins int
	JitterSecs          int
}

func NewUpdater(lastUpdated time.Time, geo GeoLookup, config Config) *Updater {
	return &Updater{
		lastUpdated: lastUpdated,
		closer:      make(chan bool),
		geo:         geo,
		ipCityPath:  os.Getenv("GEO_IP_DB"),
		ipASNPath:   os.Getenv("ASN_IP_DB"),
		config:      config,
	}
}

func writeWithRename(data io.ReadCloser, fname string) error {
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	partName := fname + ".part"
	err := ioutil.WriteFile(partName, buf.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("Error writing object to file '%s': %s", partName, err)
	}
	err = os.Rename(partName, fname)
	if err != nil {
		return fmt.Errorf("Error renaming file '%s' to '%s': %s", partName, fname, err)
	}
	return nil
}

// getIfNew downloads the new geoip dbs from s3 if there are new ones, and returns
// a boolean true if it was new
func (u *Updater) getIfNew() (bool, error) {
	svc := s3.New(session.New(&aws.Config{}))
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket:          aws.String(u.config.ConfigBucket),
		Key:             aws.String(u.config.IpCityKey),
		IfModifiedSince: aws.Time(u.lastUpdated),
	})
	if err != nil {
		switch err := err.(type) {
		case awserr.Error:
			if err.Code() == "304NotModified" {
				// Not a new geoip db
				return false, nil
			}
		}
		return false, fmt.Errorf("Error getting s3 object at 's3://%s/%s': %s", u.config.ConfigBucket, u.config.IpCityKey, err)
	}
	err = writeWithRename(resp.Body, u.ipCityPath)
	if err != nil {
		return false, err
	}

	resp, err = svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(u.config.ConfigBucket),
		Key:    aws.String(u.config.IpASNKey),
	})
	if err != nil {
		return false, fmt.Errorf("Error getting s3 object at 's3://%s/%s': %s", u.config.ConfigBucket, u.config.IpASNKey, err)
	}
	err = writeWithRename(resp.Body, u.ipASNPath)
	if err != nil {
		return false, err
	}
	return true, nil
}

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

func (u *Updater) Close() {
	u.closer <- true
}
