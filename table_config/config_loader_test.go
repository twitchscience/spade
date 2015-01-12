package table_config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/scoop_protocol/scoop_protocol"
	"github.com/twitchscience/spade/reporter"
)

var (
	knownScoopProtocolConfig1 = []scoop_protocol.Config{
		scoop_protocol.Config{
			EventName: "foo",
			Columns: []scoop_protocol.ColumnDefinition{
				scoop_protocol.ColumnDefinition{
					InboundName:           "in",
					OutboundName:          "out",
					Transformer:           "int",
					ColumnCreationOptions: "",
				},
			},
		},
	}
	knownScoopProtocolConfig2 = []scoop_protocol.Config{
		scoop_protocol.Config{
			EventName: "bar",
			Columns: []scoop_protocol.ColumnDefinition{
				scoop_protocol.ColumnDefinition{
					InboundName:           "in",
					OutboundName:          "out",
					Transformer:           "int",
					ColumnCreationOptions: "",
				},
			},
		},
	}
)

func TestRefresh(t *testing.T) {
	c, _ := statsd.NewNoop()

	dl, err := NewDynamicLoader(
		&testFetcher{
			failFetch: []bool{
				false,
				false,
			},
			configs: [][]scoop_protocol.Config{
				knownScoopProtocolConfig1,
				knownScoopProtocolConfig2,
			},
		},
		1*time.Microsecond,
		1,
		reporter.WrapCactusStatter(c, 0.1),
	)
	if err != nil {
		t.Fatalf("was expecting no error but got %v\n", err)
		t.FailNow()

	}
	_, err = dl.GetColumnsForEvent("foo")
	if err != nil {
		t.Fatal("expected to track foo")
		t.FailNow()
	}

	go dl.Crank()
	time.Sleep(101 * time.Millisecond)
	_, err = dl.GetColumnsForEvent("bar")
	if err != nil {
		t.Fatal("expected to track bar")
		t.FailNow()
	}
	_, err = dl.GetColumnsForEvent("foo")
	if err == nil {
		t.Fatal("expected to not track foo")
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
	)
	if err == nil {
		t.Fatalf("expected loader to timeout\n")
		t.FailNow()
	}
}

type testFetcher struct {
	failFetch []bool
	configs   [][]scoop_protocol.Config

	i int
}

type testReadWriteCloser struct {
	config []scoop_protocol.Config
}

func (trwc *testReadWriteCloser) Read(p []byte) (int, error) {
	var b []byte
	b, _ = json.Marshal(trwc.config)
	return copy(p, b), io.EOF
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
		return nil, errors.New(fmt.Sprintf("failed on %d try", t.i))
	}
	if len(t.configs)-1 < t.i {
		return nil, errors.New(fmt.Sprintf("failed on %d try", t.i))
	}
	rc := &testReadWriteCloser{
		config: t.configs[t.i],
	}
	t.i++
	return rc, nil
}

func (t *testFetcher) ConfigDestination(d string) (io.WriteCloser, error) {
	return &testReadWriteCloser{
		config: knownScoopProtocolConfig1,
	}, nil
}
