package fetcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/twitchscience/scoop_protocol/scoop_protocol"
)

var knownScoopProtocolSchemaEvents = []scoop_protocol.Config{
	{
		EventName: "foo",
		Columns: []scoop_protocol.ColumnDefinition{
			{
				InboundName:           "in",
				OutboundName:          "out",
				Transformer:           "foo",
				ColumnCreationOptions: "",
			},
		},
	},
}

var knownScoopProtocolKinesisConfigs = []scoop_protocol.AnnotatedKinesisConfig{
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
			Events: map[string]*scoop_protocol.KinesisWriterEventConfig{
				"event": {
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
}

func TestValidateSchema(t *testing.T) {
	if ValidateFetchedSchema([]byte{}) == nil {
		t.Error("Expected validate to error, got nil")
	}

	b := []byte("this wont work")
	if ValidateFetchedSchema(b) == nil {
		t.Error("Expected validate to error when given non-schema.Event array, got nil")
	}

	b, err := json.Marshal(knownScoopProtocolSchemaEvents)
	if err != nil {
		t.Errorf("Unexpected error serializing schema.Event: %s", err)
	}
	if ValidateFetchedSchema(b) != nil {
		t.Errorf("Expected validate to return nil, got error")
	}
}

func TestValidateKinesisConfig(t *testing.T) {
	if ValidateFetchedKinesisConfig([]byte{}) == nil {
		t.Error("Expected validate to error, got nil")
	}

	b := []byte("this wont work")
	if ValidateFetchedKinesisConfig(b) == nil {
		t.Error("Expected validate to error when given non kinesis config array, got nil")
	}

	b, err := json.Marshal(knownScoopProtocolKinesisConfigs)
	if err != nil {
		t.Errorf("Unexpected error serializing schema.Event: %s", err)
	}
	if ValidateFetchedKinesisConfig(b) != nil {
		t.Errorf("Expected validate to return nil, got error")
	}
}

type testFetcher struct {
	failFetch             bool
	failRead              bool
	bogusData             bool
	failConfigDestination bool
	failWrite             bool
}

type testReadWriteCloser struct {
	bogusData bool
	failRead  bool
	failWrite bool
}

func (trwc *testReadWriteCloser) Read(p []byte) (int, error) {
	var b []byte
	if trwc.bogusData {
		b = []byte("crap")
	} else if trwc.failRead {
		return 1, errors.New("Intentional error while reading")
	} else {
		b, _ = json.Marshal(knownScoopProtocolSchemaEvents)
	}
	return copy(p, b), io.EOF
}

func (trwc *testReadWriteCloser) Write(p []byte) (int, error) {
	if trwc.failWrite {
		return len(p), errors.New("Intentional error while writing")
	}
	return len(p), nil
}

func (trwc *testReadWriteCloser) Close() error {
	return nil
}

func (tf *testFetcher) FetchAndWrite(src io.ReadCloser, dest io.WriteCloser) error {
	f := New("foo", "bar", nil, ValidateFetchedSchema)
	return f.FetchAndWrite(src, dest)
}

func (tf *testFetcher) Fetch() (io.ReadCloser, error) {
	if tf.failFetch {
		return nil, fmt.Errorf("failed to fetch from server")
	}
	return &testReadWriteCloser{tf.bogusData, tf.failRead, tf.failWrite}, nil
}

func (tf *testFetcher) ConfigDestination(d string) (io.WriteCloser, error) {
	if tf.failConfigDestination {
		return nil, fmt.Errorf("failed to fetch config destination")
	}
	return &testReadWriteCloser{tf.bogusData, tf.failRead, tf.failWrite}, nil
}

const (
	FailOnFetch int = 1 << iota
	FailOnRead
	FailBadData
	FailConfigDest
	FailWrite
)

func makeTestFetcher(bitmask int) *testFetcher {
	return &testFetcher{
		failFetch:             bitmask&FailOnFetch != 0,
		failRead:              bitmask&FailOnRead != 0,
		bogusData:             bitmask&FailBadData != 0,
		failConfigDestination: bitmask&FailConfigDest != 0,
		failWrite:             bitmask&FailWrite != 0,
	}
}

func TestFetchConfig(t *testing.T) {
	f := makeTestFetcher(FailOnFetch)
	err := FetchConfig(f, "bar")
	if err == nil {
		t.Errorf("Expected testFetcher:%v to generate an error due to fetching issue", f)
	}

	f = makeTestFetcher(FailOnRead)
	err = FetchConfig(f, "bar")
	if err == nil {
		t.Errorf("Expected testFetcher:%v to generate an error due to reading issue", f)
	}

	f = makeTestFetcher(FailBadData)
	err = FetchConfig(f, "bar")
	if err == nil {
		t.Errorf("Expected testFetcher:%v to generate an error due to invalid data", f)
	}

	f = makeTestFetcher(FailConfigDest)
	err = FetchConfig(f, "bar")
	if err == nil {
		t.Errorf("Expected testFetcher:%v to generate an error due to config destination issue", f)
	}

	f = makeTestFetcher(FailWrite)
	err = FetchConfig(f, "bar")
	if err == nil {
		t.Errorf("Expected testFetcher:%v to generate an error due to issue while writing", f)
	}

	f = makeTestFetcher(0)
	err = FetchConfig(f, "bar")
	if err != nil {
		t.Errorf("Expected testFetcher:%v to run error free, got error: %s", f, err)
	}
}
