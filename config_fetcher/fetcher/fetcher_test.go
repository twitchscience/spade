package fetcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
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

func TestNew(t *testing.T) {
	f := New("foo")

	v := f.(*fetcher)
	if v == nil {
		t.Error("Expected New() to return fetcher struct")
	}

	expected := "foo"
	if v.url != expected {
		t.Errorf("Expected url == %s, got: %s", expected, v.url)
	}

	typeExpectation := reflect.TypeOf(&http.Client{})
	if reflect.TypeOf(v.hc) != reflect.TypeOf(&http.Client{}) {
		t.Errorf("Expected hc to be a %v, got: %v", typeExpectation, reflect.TypeOf(v.hc))
	}
}

func TestValidate(t *testing.T) {
	if validate([]byte{}) {
		t.Error("Expected validate() to return false, got true")
	}

	b := []byte("this wont work")
	if validate(b) {
		t.Error("Expected validate() to return false when given non-schema.Event array, got true")
	}

	b, err := json.Marshal(knownScoopProtocolSchemaEvents)
	if err != nil {
		t.Errorf("Unexpected error serializing schema.Event: %s", err)
	}
	if !validate(b) {
		t.Errorf("Expected validate() to return true, got false")
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
	f := New("foo")
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
