package kinesisconfigs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/writer"
)

var (
	knownScoopProtocolKinesisConfigInit, _ = json.Marshal([]scoop_protocol.AnnotatedKinesisConfig{
		{
			AWSAccount:       123123,
			Team:             "scieng",
			Version:          3,
			Contact:          "me",
			Usage:            "testing",
			ConsumingLibrary: "go test",
			SpadeConfig: scoop_protocol.KinesisWriterConfig{
				StreamName:             "foo",
				StreamRole:             "bar",
				StreamType:             "firehose",
				Compress:               false,
				FirehoseRedshiftStream: true,
				BufferSize:             1024,
				MaxAttemptsPerRecord:   10,
				RetryDelay:             "1s",
				Events: map[string]*struct {
					Filter     string
					FilterFunc func(map[string]string) bool `json:"-"`
					Fields     []string
				}{"event": {
					Filter: "",
					Fields: []string{
						"time",
						"time_utc",
					},
				},
				},

				Globber: scoop_protocol.GlobberConfig{
					MaxSize:      990000,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
				Batcher: scoop_protocol.BatcherConfig{
					MaxSize:      990000,
					MaxEntries:   500,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
			},
		},
	})

	knownScoopProtocolKinesisConfigUpdate, _ = json.Marshal([]scoop_protocol.AnnotatedKinesisConfig{
		{
			AWSAccount:       123123,
			Team:             "scieng",
			Version:          4,
			Contact:          "me",
			Usage:            "testing",
			ConsumingLibrary: "go test",
			SpadeConfig: scoop_protocol.KinesisWriterConfig{
				StreamName:             "foo",
				StreamRole:             "bar",
				StreamType:             "firehose",
				Compress:               false,
				FirehoseRedshiftStream: true,
				BufferSize:             1024,
				MaxAttemptsPerRecord:   10,
				RetryDelay:             "1s",
				Events: map[string]*struct {
					Filter     string
					FilterFunc func(map[string]string) bool `json:"-"`
					Fields     []string
				}{"event": {
					Filter: "",
					Fields: []string{
						"time",
					},
				},
				},

				Globber: scoop_protocol.GlobberConfig{
					MaxSize:      990000,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
				Batcher: scoop_protocol.BatcherConfig{
					MaxSize:      990000,
					MaxEntries:   500,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
			},
		},
	})

	knownScoopProtocolKinesisConfigAdd, _ = json.Marshal([]scoop_protocol.AnnotatedKinesisConfig{
		{
			AWSAccount:       123123,
			Team:             "scieng",
			Version:          1,
			Contact:          "me",
			Usage:            "testing",
			ConsumingLibrary: "go test",
			SpadeConfig: scoop_protocol.KinesisWriterConfig{
				StreamName:           "foo",
				StreamRole:           "bar",
				StreamType:           "stream",
				Compress:             false,
				BufferSize:           1024,
				MaxAttemptsPerRecord: 10,
				RetryDelay:           "1s",
				Events: map[string]*struct {
					Filter     string
					FilterFunc func(map[string]string) bool `json:"-"`
					Fields     []string
				}{"event": {
					Filter: "",
					Fields: []string{
						"time",
						"time_utc",
					},
				},
				},

				Globber: scoop_protocol.GlobberConfig{
					MaxSize:      990000,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
				Batcher: scoop_protocol.BatcherConfig{
					MaxSize:      990000,
					MaxEntries:   500,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
			},
		},
		{
			AWSAccount:       123123,
			Team:             "scieng",
			Version:          4,
			Contact:          "me",
			Usage:            "testing",
			ConsumingLibrary: "go test",
			SpadeConfig: scoop_protocol.KinesisWriterConfig{
				StreamName:             "foo",
				StreamRole:             "bar",
				StreamType:             "firehose",
				Compress:               false,
				FirehoseRedshiftStream: true,
				BufferSize:             1024,
				MaxAttemptsPerRecord:   10,
				RetryDelay:             "1s",
				Events: map[string]*struct {
					Filter     string
					FilterFunc func(map[string]string) bool `json:"-"`
					Fields     []string
				}{"event": {
					Filter: "",
					Fields: []string{
						"time",
					},
				},
				},

				Globber: scoop_protocol.GlobberConfig{
					MaxSize:      990000,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
				Batcher: scoop_protocol.BatcherConfig{
					MaxSize:      990000,
					MaxEntries:   500,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
			},
		},
	})
	knownScoopProtocolKinesisConfigDelete, _ = json.Marshal([]scoop_protocol.AnnotatedKinesisConfig{
		{
			AWSAccount:       123123,
			Team:             "scieng",
			Version:          1,
			Contact:          "me",
			Usage:            "testing",
			ConsumingLibrary: "go test",
			SpadeConfig: scoop_protocol.KinesisWriterConfig{
				StreamName:           "foo",
				StreamRole:           "bar",
				StreamType:           "stream",
				Compress:             false,
				BufferSize:           1024,
				MaxAttemptsPerRecord: 10,
				RetryDelay:           "1s",
				Events: map[string]*struct {
					Filter     string
					FilterFunc func(map[string]string) bool `json:"-"`
					Fields     []string
				}{"event": {
					Filter: "",
					Fields: []string{
						"time",
						"time_utc",
					},
				},
				},

				Globber: scoop_protocol.GlobberConfig{
					MaxSize:      990000,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
				Batcher: scoop_protocol.BatcherConfig{
					MaxSize:      990000,
					MaxEntries:   500,
					MaxAge:       "1s",
					BufferLength: 1024,
				},
			},
		},
	})
)

func TestRefresh(t *testing.T) {
	c, _ := statsd.NewNoop()

	dl, err := NewDynamicLoader(
		&testFetcher{
			failFetch: []bool{
				false,
				false,
				false,
				false,
			},
			configs: [][]byte{
				knownScoopProtocolKinesisConfigInit,
				knownScoopProtocolKinesisConfigUpdate,
				knownScoopProtocolKinesisConfigAdd,
				knownScoopProtocolKinesisConfigDelete,
			},
		},
		1*time.Microsecond,
		1,
		reporter.WrapCactusStatter(c, 0.1),
		&testMultee{},
		session.New(),
		func(*session.Session, statsd.Statter, scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error) {
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("was expecting no error but got %v\n", err)
		t.FailNow()

	}

	if len(dl.configs) != 1 {
		t.Fatal("expected to have 1 config right now")
		t.FailNow()
	}

	go dl.Crank()
	time.Sleep(time.Second)
	if len(dl.configs) != 1 {
		t.Fatal("expected to have 1 config right now")
		t.FailNow()
	}

	if dl.configs[0].SpadeConfig.StreamType != "stream" {
		t.Fatal("expected to have a stream left after delete")
		t.FailNow()
	}

	dl.closer <- true
}

func TestRetryPull(t *testing.T) {
	c, _ := statsd.NewNoop()
	_, err := NewDynamicLoader(
		&testFetcher{
			failFetch: []bool{
				true,
				true,
				true,
				true,
				true,
			},
		},
		1*time.Second,
		1*time.Microsecond,
		reporter.WrapCactusStatter(c, 0.1),
		&testMultee{},
		session.New(),
		func(*session.Session, statsd.Statter, scoop_protocol.KinesisWriterConfig) (writer.SpadeWriter, error) {
			return nil, nil
		},
	)
	if err == nil {
		t.Fatalf("expected loader to timeout\n")
		t.FailNow()
	}
}

type testMultee struct {
}

type testFetcher struct {
	failFetch []bool
	configs   [][]byte

	i int
}

type testReadWriteCloser struct {
	config    []byte
	readSoFar int
}

func (trwc *testReadWriteCloser) Read(p []byte) (int, error) {
	if len(p) <= len(trwc.config)-trwc.readSoFar {
		trwc.readSoFar += bytes.MinRead
		return copy(p, trwc.config[trwc.readSoFar-bytes.MinRead:trwc.readSoFar]), nil
	}

	return copy(p, trwc.config[trwc.readSoFar:]), io.EOF
}

func (trwc *testReadWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (trwc *testReadWriteCloser) Close() error {
	return nil
}

func (t *testFetcher) FetchAndWrite(r io.ReadCloser, w io.WriteCloser) error {
	return nil
}

func (t *testFetcher) Fetch() (io.ReadCloser, error) {
	if len(t.failFetch) < t.i && t.failFetch[t.i] {
		t.i++
		return nil, fmt.Errorf("failed on %d try", t.i)
	}
	if len(t.configs)-1 < t.i {
		return nil, fmt.Errorf("failed on %d try", t.i)
	}
	rc := &testReadWriteCloser{
		config: t.configs[t.i],
	}
	t.i++
	return rc, nil
}

func (t *testFetcher) ConfigDestination(d string) (io.WriteCloser, error) {
	return &testReadWriteCloser{
		config: knownScoopProtocolKinesisConfigInit,
	}, nil
}

func (t *testMultee) Add(key string, w writer.SpadeWriter) {
}

func (t *testMultee) Drop(key string) {
}

func (t *testMultee) Replace(key string, w writer.SpadeWriter) {
}
