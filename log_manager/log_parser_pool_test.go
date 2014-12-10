package log_manager

import (
	"reflect"
	"sync"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/processor"
	"github.com/twitchscience/spade/reader"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"

	"testing"
	"time"
)

var (
	_config = table_config.NewStaticLoader(
		map[string][]transformer.RedshiftType{
			"login": []transformer.RedshiftType{
				transformer.RedshiftType{transformer.GetTransform("float"), "sampling_factor"},
				transformer.RedshiftType{transformer.GetTransform("varchar"), "distinct_id"},
				transformer.RedshiftType{transformer.GetTransform("f@timestamp@unix"), "time"},
				transformer.RedshiftType{transformer.GetTransform("f@timestamp@unix"), "client_time"},
			},
		},
	)
	_transformer transformer.Transformer = transformer.NewRedshiftTransformer(_config)
)

//////////////////////////////////
//
//  Helper test functions
//

type testWriter struct {
	report reporter.Reporter
}

func (w *testWriter) Write(r *writer.WriteRequest) error {
	w.report.Record(r.GetResult())
	return nil
}

func (w *testWriter) Close() error {
	return nil
}

func (w *testWriter) Reset() error {
	return nil
}

func buildTestWriter(r reporter.Reporter) writer.SpadeWriter {
	return &testWriter{
		report: r,
	}
}

type testTracker struct {
	results []reporter.Result
}

func (t *testTracker) Track(res *reporter.Result) {
	t.results = append(t.results, *res)
}

func buildTestReporter() reporter.Reporter {
	return reporter.BuildSpadeReporter(&sync.WaitGroup{}, []reporter.Tracker{
		&testTracker{
			results: make([]reporter.Result, 5),
		},
	})
}

type testReader struct {
	lines []parser.ParseRequest
	pos   int
	lock  sync.Mutex
}

func (r *testReader) ProvideLine() (*parser.ParseRequest, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	defer func() { r.pos++ }()
	if r.pos >= len(r.lines) {
		return nil, reader.EOF{}
	}
	r.lines[r.pos].Pstart = time.Now()
	return &r.lines[r.pos], nil
}

func (r *testReader) Close() error {
	return nil
}

///////////////////////////////////
//
//  Tests
//
func TestLogParser(t *testing.T) {
	r := buildTestReporter()
	parser := parser.BuildSpadeParser(r)
	writer := buildTestWriter(r)

	pool := processor.BuildTestPool(1, 1, parser, _transformer)
	pool.Listen(writer)

	testParser := LogParser{
		Reporter:  r,
		Processor: pool,
		Writer:    writer,
	}

	// Set up reader
	reader := &testReader{
		lines: testLines,
	}
	// run test
	output, err := testParser.parse(reader)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expected := map[string]int{
		reporter.SKIPPED_COLUMN.String():       4,
		reporter.UNABLE_TO_PARSE_DATA.String(): 1,
	}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Expected %+v but got %+v\n", expected, output)
		t.FailNow()
	}
}

var testLines = []parser.ParseRequest{
	parser.ParseRequest{
		Target: []byte("222.222.222.222 [1418172623.000] data=eyJldmVudCI6ImxvZ2luIiwicHJvcGVydGllcyI6eyJkaXN0aW5jdF9pZCI6ImZmZmZmZmZmZmZmZmZmZmZmZiIsInNhbXBsaW5nX2ZhY3RvciI6MC41fX0= fffff-fffff-fffff-1 recordversion=1"),
	},
	parser.ParseRequest{
		Target: []byte("222.222.222.222 [1418172623.000] data=eyJldmVudCI6Imxv Z2luIiwicHJvcGVydGllcyI6eyJkaXN0aW5jdF9pZCI6ImZmZmZmZmZmZmZmZmZmZmZmZiIsInNhbXBsaW5nX2ZhY3RvciI6MC41fX0= fffff-fffff-fffff-2 recordversion=1"),
	},
	parser.ParseRequest{
		Target: []byte("222.222.222.222 [1418172623.000] data=W3siZXZlbnQiOiJsb2dpbiIsInByb3BlcnRpZXMiOnsiZGlzdGluY3RfaWQiOiJmZmZmZmZmZmZmZmZmZmZmZmYiLCJzYW1wbGluZ19mYWN0b3IiOjAuNX19LHsiZXZlbnQiOiJsb2dpbiIsInByb3BlcnRpZXMiOnsiZGlzdGluY3RfaWQiOiJmZmZmZmZmZmZmZmZmZmZmZmYiLCJzYW1wbGluZ19mYWN0b3IiOjAuNX19LHsiZXZlbnQiOiJsb2dpbiIsInByb3BlcnRpZXMiOnsiZGlzdGluY3RfaWQiOiJmZmZmZmZmZmZmZmZmZmZmZmYiLCJzYW1wbGluZ19mYWN0b3IiOjAuNX19XQ== fffff-fffff-fffff-3 recordversion=1"),
	},
}
