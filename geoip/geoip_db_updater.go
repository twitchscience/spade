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
	lastUpdated    time.Time
	reloadTime     time.Duration
	closer         chan bool
	geo            GeoLookup
	spadeDir       string
	s3ConfigPrefix string
}

func NewUpdater(lastUpdated time.Time, reloadTime time.Duration, geo GeoLookup, s3ConfigPrefix string) Updater {
	u := Updater{
		lastUpdated:    lastUpdated,
		reloadTime:     reloadTime,
		closer:         make(chan bool),
		geo:            geo,
		s3ConfigPrefix: s3ConfigPrefix,
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
	svc := s3.New(session.New(&aws.Config{Region: aws.String("us-west-2")}))
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket:          aws.String("twitch-aws-config"),
		Key:             aws.String(u.s3ConfigPrefix + "GeoIPCity.dat"),
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
		return false, fmt.Errorf("Error getting s3 object: %s", err)
	}
	err = writeWithRename(resp.Body, u.spadeDir+"/config/GeoIPCity.dat")
	if err != nil {
		return false, err
	}

	resp, err = svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("twitch-aws-config"),
		Key:    aws.String(u.s3ConfigPrefix + "GeoLiteASNum.dat"),
	})
	if err != nil {
		return false, fmt.Errorf("Error getting s3 object: %s", err)
	}
	writeWithRename(resp.Body, u.spadeDir+"/config/GeoLiteASNum.dat")
	if err != nil {
		return false, err
	}
	return true, nil
}

func (u Updater) UpdateLoop() {
	tick := time.NewTicker(u.reloadTime)
	for {
		select {
		case <-tick.C:
			jitter := time.Duration(rand.Intn(100)) * time.Second
			time.Sleep(jitter)
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
			}

		case <-u.closer:
			return
		}
	}
}

func (u Updater) Close() {
	u.closer <- true
}
