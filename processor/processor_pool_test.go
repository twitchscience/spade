package processor

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/twitchscience/spade/parser"
	jsonLog "github.com/twitchscience/spade/parser/json"
	"github.com/twitchscience/spade/reporter"
	tableConfig "github.com/twitchscience/spade/tables"
	"github.com/twitchscience/spade/transformer"
	"github.com/twitchscience/spade/writer"

	"io/ioutil"
	"testing"
	"time"
)

var (
	sampleLogLine      = loadFile("test_resources/sample_logline.txt")
	sampleErrorLogLine = loadFile("test_resources/sample_error_logline.txt")
	sampleMultiLogLine = loadFile("test_resources/sample_multi_logline.txt")
	expectedJSONBytes  = loadFile("test_resources/expected_byte_buffer.txt")
	PST                = getPST()

	_config = tableConfig.NewStaticLoader(
		map[string][]transformer.RedshiftType{
			"login": {
				{
					Transformer: transformer.GetTransform("float"),
					InboundName: "sampling_factor",
				},
				{
					Transformer: transformer.GetTransform("varchar"),
					InboundName: "distinct_id",
				},
				{
					Transformer: transformer.GetTransform("f@timestamp@unix"),
					InboundName: "time",
				},
				{
					Transformer: transformer.GetTransform("f@timestamp@unix"),
					InboundName: "client_time",
				},
			},
		},
		map[string]int{
			"login": 42,
		},
	)
	_transformer = transformer.NewRedshiftTransformer(_config)
	_parser      = parser.BuildSpadeParser(&dummyReporter{})
)

func getPST() *time.Location {
	pst, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	return pst
}

func loadFile(file string) string {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return ""
	}
	return string(b)
}

///////////////////////////////////////////
//
//  Test Mockers
//
type _panicParser struct{}

func (p *_panicParser) Parse(parser.Parseable) ([]parser.MixpanelEvent, error) {
	panic("panicked!")
}

type _panicTransformer struct{}

func (p *_panicTransformer) Consume(*parser.MixpanelEvent) *writer.WriteRequest {
	panic("panicked!")
}

type DummyWriter struct {
}

func (d *DummyWriter) Write(w *writer.WriteRequest) {
}

func (d *DummyWriter) Close() error {
	return nil
}

type testWriter struct {
	m        *sync.Mutex
	requests []*writer.WriteRequest
}

func (w *testWriter) Write(r *writer.WriteRequest) {
	w.m.Lock()
	defer w.m.Unlock()
	w.requests = append(w.requests, r)
}

func (w *testWriter) Close() error {
	return nil
}

func (w *testWriter) Rotate() (bool, error) {
	w.requests = make([]*writer.WriteRequest, 0, 1)
	return true, nil
}

type benchTestWriter struct {
	r chan *writer.WriteRequest
}

func (w *benchTestWriter) Write(r *writer.WriteRequest) {
	w.r <- r
}

func (w *benchTestWriter) Close() error {
	close(w.r)
	return nil
}

func (w *benchTestWriter) Rotate() (bool, error) {
	return true, nil
}

type dummyReporter struct{}

func (d *dummyReporter) Record(c *reporter.Result) {}
func (d *dummyReporter) IncrementExpected(n int)   {}
func (d *dummyReporter) Reset()                    {}
func (d *dummyReporter) Finalize() map[string]int {
	return make(map[string]int)
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

//////////////////////////////////
//
//  Helper test functions
//
func buildTestPool(nConverters, nTransformers int, p parser.Parser, t transformer.Transformer,
	w writer.SpadeWriter) *SpadeProcessorPool {
	transformers := make([]*RequestTransformer, nTransformers)
	converters := make([]*RequestConverter, nConverters)

	requestChannel := make(chan parser.Parseable, QueueSize)
	transport := make(chan parser.MixpanelEvent, QueueSize)

	for i := 0; i < nConverters; i++ {
		converters[i] = &RequestConverter{
			parser: p,
			in:     requestChannel,
			out:    transport,
			closer: make(chan bool),
		}
	}

	for i := 0; i < nTransformers; i++ {
		transformers[i] = &RequestTransformer{
			t:      t,
			in:     transport,
			closer: make(chan bool),
		}
	}

	return &SpadeProcessorPool{
		in:           requestChannel,
		converters:   converters,
		transformers: transformers,
		writer:       w,
	}
}

func requestEqual(r1, r2 *writer.WriteRequest) bool {
	return r1.Category == r2.Category &&
		r1.Line == r2.Line &&
		r1.UUID == r2.UUID &&
		bytes.Equal(r1.Source, r2.Source) &&
		r1.Failure == r2.Failure &&
		r1.Pstart.Equal(r2.Pstart)
}

///////////////////////////////////////
//
//  Tests
//
func init() {
	if err := jsonLog.Register(false); err != nil {
		fmt.Fprintf(os.Stderr, "Could not register jsonLog: %v\n", err)
		os.Exit(1)
	}
}

func TestPanicRecoveryProcessing(t *testing.T) {
	now := time.Now().In(PST)
	rawLine := `{"clientIp": "10.1.40.26", "data": "eyJldmVudCIgOiJsb2dpbiJ9", "uuid": "uuid1"}`
	_exampleRequest := &parseRequest{
		[]byte(rawLine),
		now,
	}
	expectedPP := writer.WriteRequest{
		Category: "Unknown",
		Version:  0,
		Line:     "",
		UUID:     "error",
		Source:   []byte(rawLine),
		Failure:  reporter.PanickedInProcessing,
		Pstart:   now,
	}
	expectedPT := writer.WriteRequest{
		Category: "Unknown",
		Version:  0,
		Line:     "",
		UUID:     "error",
		Source:   []byte{},
		Failure:  reporter.PanickedInProcessing,
		Pstart:   now,
	}

	w := &testWriter{
		m:        &sync.Mutex{},
		requests: make([]*writer.WriteRequest, 0, 2),
	}
	pP := buildTestPool(1, 1, &_panicParser{}, _transformer, w)
	pP.StartListeners()
	pP.Process(_exampleRequest)

	pT := buildTestPool(1, 1, _parser, &_panicTransformer{}, w)
	pT.StartListeners()
	pT.Process(_exampleRequest)

	time.Sleep(time.Second) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()

	if len(w.requests) != 2 {
		t.Logf("expected 2 results got %d", len(w.requests))
		t.FailNow()
	}
	if !requestEqual(&expectedPP, w.requests[0]) {
		if !requestEqual(&expectedPP, w.requests[1]) {
			t.Logf("Expected\n%+v\nbut got\n%+v\n", expectedPP, *w.requests[1])
			t.Fail()
		}
		if !requestEqual(&expectedPT, w.requests[0]) {
			t.Logf("Expected\n%+v\nbut got\n%+v\n", expectedPT, *w.requests[1])
			t.Fail()
		}
	} else if !requestEqual(&expectedPT, w.requests[1]) {
		t.Logf("Expected\n%+v\nbut got\n%+v\n", expectedPT, *w.requests[1])
		t.Fail()
	}
}

func TestEmptyPropertyProcessing(t *testing.T) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(`{"receivedAt": "2013-10-17T18:05:55.338Z", "clientIp": "10.1.40.26", ` +
			`"data": "eyJldmVudCI6ImxvZ2luIiwicHJvcGVydGllcyI6e319", "uuid": "uuid1", ` +
			`"recordversion": 3}`),
		now,
	}
	logTime := time.Unix(1382033155, 0)
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"\"\t\"\"\t\"" + logTime.In(PST).Format(transformer.RedshiftDatetimeIngestString) + "\"\t\"\"",
		UUID:     "uuid1",
		Source:   []byte("{}"),
		Failure:  reporter.SkippedColumn,
		Pstart:   now,
	}

	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}

	p := buildTestPool(1, 1, _parser, _transformer, w)
	p.StartListeners()
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()

	if len(w.requests) != 1 {
		t.Logf("expected 1 result got %d", len(w.requests))
		t.FailNow()
	}

	if !requestEqual(&expected, w.requests[0]) {
		t.Logf("Expected %+v but got %+v\n", expected, w.requests[0])
		t.Fail()
	}
}

func TestRequestProcessing(t *testing.T) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(sampleLogLine),
		now,
	}
	expected := writer.WriteRequest{
		Category: "login",
		Version:  42,
		Line:     "\"0.1500000059604645\"\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t\"2013-10-17 11:05:55\"\t\"2013-09-30 17:00:02\"",
		UUID:     "uuid1",
		Source:   []byte(expectedJSONBytes),
		Pstart:   now,
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := buildTestPool(1, 1, _parser, _transformer, w)
	p.StartListeners()
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) != 1 {
		t.Logf("expected 1 result got %d", len(w.requests))
		t.FailNow()
	}

	if !requestEqual(&expected, w.requests[0]) {
		t.Logf("Expected %+v but got %+v\n", expected, w.requests[0])
		t.Fail()
	}
}

func TestErrorRequestProcessing(t *testing.T) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(sampleErrorLogLine),
		now,
	}
	expected := writer.WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "uuid1",
		Source:   nil,
		Pstart:   now,
		Failure:  reporter.UnableToParseData,
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := buildTestPool(1, 1, _parser, _transformer, w)
	p.StartListeners()
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) != 1 {
		t.Logf("expected 1 result got %d", len(w.requests))
		t.FailNow()
	}

	if !requestEqual(&expected, w.requests[0]) {
		t.Logf("Expected %+v but got %+v\n", expected, w.requests[0])
		t.Fail()
	}
}

func TestMultiRequestProcessing(t *testing.T) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(sampleMultiLogLine),
		now,
	}
	expected := map[string]*writer.WriteRequest{
		"uuid1-0": {
			Category: "login",
			Version:  42,
			Line:     "\"0.1500000059604645\"\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t\"2013-10-17 11:05:55\"\t\"2013-09-30 17:00:02\"",
			UUID:     "uuid1-0",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		"uuid1-1": {
			Category: "login",
			Version:  42,
			Line:     "\"0.1500000059604645\"\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t\"2013-10-17 11:05:55\"\t\"2013-09-30 17:00:02\"",
			UUID:     "uuid1-1",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		"uuid1-2": {
			Category: "login",
			Version:  42,
			Line:     "\"0.1500000059604645\"\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t\"2013-10-17 11:05:55\"\t\"2013-09-30 17:00:02\"",
			UUID:     "uuid1-2",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		"uuid1-3": {
			Category: "login",
			Version:  42,
			Line:     "\"0.1500000059604645\"\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t\"2013-10-17 11:05:55\"\t\"2013-09-30 17:00:02\"",
			UUID:     "uuid1-3",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := buildTestPool(5, 30, _parser, _transformer, w)
	p.StartListeners()
	p.Process(_exampleRequest)

	time.Sleep(time.Second) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) != len(expected) {
		t.Logf("expected %d results got %d\n", len(expected), len(w.requests))
		t.Fail()
	}

	for _, e := range w.requests {
		if expected[e.UUID] == nil {
			t.Logf("Unknown or duplicate UUID: %s\n", e.UUID)
			t.Fail()
			continue
		}
		if !requestEqual(expected[e.UUID], e) {
			fmt.Println(string(e.Source))
			t.Logf("Expected %+v but got %+v\n", expected[e.UUID], e)
			t.Fail()
		}
		expected[e.UUID] = nil // Ensure we don't get the same event twice.

	}
}

// Use to figure out how many converters vs transformers we need
func BenchmarkRequestProcessing(b *testing.B) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(sampleLogLine),
		now,
	}
	w := &benchTestWriter{
		r: make(chan *writer.WriteRequest),
	}

	rp := buildTestPool(15, 30, _parser, _transformer, w)
	rp.StartListeners()

	b.ReportAllocs()
	b.ResetTimer()
	wait := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wait.Add(1)
		go func() {
			for j := 0; j < QueueSize*2; j++ {
				<-w.r
			}
			wait.Done()
		}()
		for j := 0; j < QueueSize*2; j++ {
			rp.Process(_exampleRequest)
		}
		wait.Wait()
	}
}
