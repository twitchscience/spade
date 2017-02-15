package geoip

import (
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	ipstring  = "ipdbcontents"
	asnstring = "asndbcontents"
	ippath    = "iptest.db"
	asnpath   = "asntest.db"
)

type S3APINewDBs struct {
	s3iface.S3API
}

// GetObject here will return an ouput with bodies.
func (s *S3APINewDBs) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	str := "This totally won't match anything, so if the key is unknown, the test will fail down there."
	switch *goi.Key {
	case "ip":
		str = ipstring
	case "asn":
		str = asnstring
	}
	return &s3.GetObjectOutput{Body: ioutil.NopCloser(strings.NewReader(str))}, nil
}

type S3APIOldDBs struct {
	s3iface.S3API
}

// GetObject here will return a 304 not modified
func (s *S3APIOldDBs) GetObject(goi *s3.GetObjectInput) (goo *s3.GetObjectOutput, err error) {
	goo = &s3.GetObjectOutput{Body: ioutil.NopCloser(strings.NewReader(""))}
	err = awserr.NewRequestFailure(awserr.New("304", "Not Modified", errors.New("asdf")), 304, "asdf")
	return goo, err
}

func clearDBFiles() {
	_ = os.Remove(ippath)
	_ = os.Remove(asnpath)
}

func checkFileIs(path, contents string) (bool, error) {
	b, err := ioutil.ReadFile(path)
	return string(b) == contents, err
}

func initUpdater(s s3iface.S3API) *Updater {
	conf := Config{
		ConfigBucket:        "this-bucket-is-faker-than-drakes-rap-rivals",
		IPCityKey:           "ip",
		IPASNKey:            "asn",
		UpdateFrequencyMins: 1,
		JitterSecs:          1,
	}
	geoIPUpdater := NewUpdater(time.Now(), nil, conf, s)

	// Have to overwrite this here since the updater gets it from env vars
	geoIPUpdater.ipCityPath = ippath
	geoIPUpdater.ipASNPath = asnpath
	return geoIPUpdater
}

func TestUpdaterUpdates(t *testing.T) {
	geoIPUpdater := initUpdater(&S3APINewDBs{})
	updated, err := geoIPUpdater.getIfNew()
	defer clearDBFiles()
	if !updated {
		t.Error("Geoipupdater didn't update!")
	}
	if err != nil {
		t.Error(err)
	}
	if success, err := checkFileIs(ippath, ipstring); !success || err != nil {
		t.Error("IP db didn't get updated!", success, err)
	}
	if success, err := checkFileIs(asnpath, asnstring); !success || err != nil {
		t.Error("ASN db didn't get updated!", success, err)
	}
}

func TestUpdaterPassesOn304(t *testing.T) {
	geoIPUpdater := initUpdater(&S3APIOldDBs{})
	updated, err := geoIPUpdater.getIfNew()
	defer clearDBFiles()
	if updated {
		t.Error("Geoipupdater updated but shouldn't have!")
	}
	if err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(ippath); err == nil {
		t.Error("IP db was written to but shouldn't have been!")
	}
	if _, err := os.Stat(asnpath); err == nil {
		t.Error("ASN db was written to but shouldn't have been!")
	}
}
