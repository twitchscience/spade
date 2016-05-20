package geoip

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

func NewUpdater(lastUpdated time.Time, geo GeoLookup, config Config) Updater {
	u := Updater{
		lastUpdated: lastUpdated,
		closer:      make(chan bool),
		geo:         geo,
		ipCityPath:  os.Getenv("GEO_IP_DB"),
		ipASNPath:   os.Getenv("ASN_IP_DB"),
		config:      config,
	}
	return u
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
func (u Updater) getIfNew() (bool, error) {
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

func (u Updater) UpdateLoop() {
	tick := time.NewTicker(time.Duration(u.config.UpdateFrequencyMins) * time.Minute)
	for {
		select {
		case <-tick.C:
			jitter := time.Duration(rand.Intn(u.config.JitterSecs)) * time.Second
			time.Sleep(jitter)
			log.Printf("Pulling down new geoip dbs if they are new...")
			newDB, err := u.getIfNew()
			if err != nil {
				log.Printf("Error getting the new geoip: %s", err.Error())
				continue
			}
			if newDB {
				err := u.geo.Reload()
				if err != nil {
					log.Printf("Error reloading geo db: %s", err)
					continue
				}
				u.lastUpdated = time.Now()
				log.Printf("Loaded and using new geoip databases.")
			} else {
				log.Printf("... geoip dbs are not new, waiting %v minutes to try again", u.config.UpdateFrequencyMins)
			}

		case <-u.closer:
			return
		}
	}
}

func (u Updater) Close() {
	u.closer <- true
}
