package log_manager

import (
	"io/ioutil"
	"reflect"
	"sync"

	"github.com/twitchscience/spade/parser"
	nginx "github.com/twitchscience/spade/parser/server_log"
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
				transformer.RedshiftType{
					Transformer:   transformer.GetTransform("float"),
					EventProperty: "sampling_factor",
				},
				transformer.RedshiftType{
					Transformer:   transformer.GetTransform("varchar"),
					EventProperty: "distinct_id",
				},
				transformer.RedshiftType{
					Transformer:   transformer.GetTransform("f@timestamp@unix"),
					EventProperty: "time",
				},
				transformer.RedshiftType{
					Transformer:   transformer.GetTransform("f@timestamp@unix"),
					EventProperty: "client_time",
				},
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
	lines []parser.Parseable
	pos   int
	lock  sync.Mutex
}

func (r *testReader) ProvideLine() (parser.Parseable, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	defer func() { r.pos++ }()
	if r.pos >= len(r.lines) {
		return nil, reader.EOF{}
	}
	r.lines[r.pos].(*parseRequest).start = time.Now()
	return r.lines[r.pos], nil
}

func (r *testReader) Close() error {
	return nil
}

type parseRequest struct {
	data  []byte
	start time.Time
}

func (p *parseRequest) Data() []byte {
	return p.data
}

func (p *parseRequest) StartTime() time.Time {
	return p.start
}

///////////////////////////////////
//
//  Tests
//
func init() {
	nginx.Register() // TODO: replace with TestMain() in go 1.4
}

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

func loadFile(file string) []byte {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}
	return b
}

var (
	testLines = []parser.Parseable{
		// contains 1 line
		&parseRequest{
			data: loadFile("test_resources/single_line.txt"),
		},
		// should be a UNABLE_TO_PARSE_DATA error
		&parseRequest{
			data: loadFile("test_resources/broken_line.txt"),
		},
		// Contains 3 lines
		&parseRequest{
			data: loadFile("test_resources/multi_line.txt"),
		},
	}
)
