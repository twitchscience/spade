package fetcher

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testFetcher struct {
	failFetch bool
	failRead  bool
}

type testReadCloser struct {
	failRead bool
}

func (trwc *testReadCloser) Read(p []byte) (int, error) {
	if trwc.failRead {
		return 0, errors.New("Intentional error while reading")
	}
	return 0, io.EOF
}

func (trwc *testReadCloser) Close() error {
	return nil
}

func (tf *testFetcher) Fetch() (io.ReadCloser, error) {
	if tf.failFetch {
		return nil, fmt.Errorf("failed to fetch from server")
	}
	return &testReadCloser{tf.failRead}, nil
}

const (
	FailOnFetch int = 1 << iota
	FailOnRead
)

func makeTestFetcher(bitmask int) *testFetcher {
	return &testFetcher{
		failFetch: bitmask&FailOnFetch != 0,
		failRead:  bitmask&FailOnRead != 0,
	}
}

func TestFetchConfig(t *testing.T) {
	f := makeTestFetcher(FailOnFetch)
	_, err := f.Fetch()
	assert.Error(t, err, "Expected an error due to fetching issue")

	f = makeTestFetcher(FailOnRead)
	reader, err := f.Fetch()
	assert.NoError(t, err)
	_, err = ioutil.ReadAll(reader)
	assert.Error(t, err, "Expected an error due to reading issue")

	f = makeTestFetcher(0)
	reader, err = f.Fetch()
	assert.NoError(t, err)
	_, err = ioutil.ReadAll(reader)
	assert.NoError(t, err)
}
