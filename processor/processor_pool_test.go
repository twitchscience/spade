package processor

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/twitchscience/spade/parser"
	nginx "github.com/twitchscience/spade/parser/server_log"
	"github.com/twitchscience/spade/reporter"
	"github.com/twitchscience/spade/table_config"
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
	_parser                              = parser.BuildSpadeParser(&dummyReporter{})
)

func getPST() *time.Location {
	PST, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	return PST
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
	panic("paniced!")
}

type _panicTransformer struct{}

func (p *_panicTransformer) Consume(*parser.MixpanelEvent) *writer.WriteRequest {
	panic("paniced!")
}

type DummyWriter struct {
}

func (d *DummyWriter) Write(w *writer.WriteRequest) error {
	return nil
}

func (d *DummyWriter) Close() error {
	return nil
}

type testWriter struct {
	m        *sync.Mutex
	requests []*writer.WriteRequest
}

func (w *testWriter) Write(r *writer.WriteRequest) error {
	w.m.Lock()
	defer w.m.Unlock()
	w.requests = append(w.requests, r)
	return nil
}

func (w *testWriter) Close() error {
	return nil
}

func (w *testWriter) Reset() error {
	w.requests = make([]*writer.WriteRequest, 0, 1)
	return nil
}

type benchTestWriter struct {
	r chan *writer.WriteRequest
}

func (w *benchTestWriter) Write(r *writer.WriteRequest) error {
	w.r <- r
	return nil
}

func (w *benchTestWriter) Close() error {
	close(w.r)
	return nil
}

func (w *benchTestWriter) Reset() error {
	return nil
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
func buildTestPool(nConverters, nTransformers int, p parser.Parser, t transformer.Transformer) *SpadeProcessorPool {
	transformers := make([]*RequestTransformer, nTransformers)
	converters := make([]*RequestConverter, nConverters)

	requestChannel := make(chan parser.Parseable, QUEUE_SIZE)
	transport := NewGobTransport(NewBufferedTransport())

	for i := 0; i < nConverters; i++ {
		converters[i] = &RequestConverter{
			parser: p,
			in:     requestChannel,
			T:      transport,
			closer: make(chan bool),
		}
	}

	for i := 0; i < nTransformers; i++ {
		transformers[i] = &RequestTransformer{
			t:      t,
			T:      transport,
			closer: make(chan bool),
		}
	}

	return &SpadeProcessorPool{
		in:           requestChannel,
		converters:   converters,
		transformers: transformers,
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
	nginx.Register() // TODO: replace with TestMain() in go 1.4
}

func TestPanicRecoveryProcessing(t *testing.T) {
	now := time.Now().In(PST)
	rawLine := `10.1.40.26 [1382033155.388] "ip=0&data=eyJldmVudCIgOiJsb2dpbiJ9" uuid1`
	_exampleRequest := &parseRequest{
		[]byte(rawLine),
		now,
	}
	expectedPP := writer.WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "error",
		Source:   []byte(rawLine),
		Failure:  reporter.PANICED_IN_PROCESSING,
		Pstart:   now,
	}
	expectedPT := writer.WriteRequest{
		Category: "Unknown",
		Line:     "",
		UUID:     "error",
		Source:   []byte{},
		Failure:  reporter.PANICED_IN_PROCESSING,
		Pstart:   now,
	}

	w := &testWriter{
		m:        &sync.Mutex{},
		requests: make([]*writer.WriteRequest, 0, 2),
	}
	pP := BuildTestPool(1, 1, &_panicParser{}, _transformer)
	pP.Listen(w)
	pP.Process(_exampleRequest)

	pT := BuildTestPool(1, 1, _parser, &_panicTransformer{})
	pT.Listen(w)
	pT.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()

	if len(w.requests) != 2 {
		t.Logf("expeceted 2 results got", len(w.requests))
		t.Fail()
	}
	if !requestEqual(&expectedPP, w.requests[0]) {
		t.Logf("Expected\n%+v\nbut got\n%+v\n", &expectedPP, w.requests[0])
		t.Fail()
	}

	if !requestEqual(&expectedPT, w.requests[1]) {
		t.Logf("Expected\n%+v\nbut got\n%+v\n", expectedPT, *w.requests[1])
		t.Fail()
	}
}

func TestEmptyPropertyProcessing(t *testing.T) {
	now := time.Now().In(PST)
	_exampleRequest := &parseRequest{
		[]byte(`10.1.40.26 [1382033155.388] "ip=0&data=eyJldmVudCIgOiJsb2dpbiJ9" uuid1`),
		now,
	}
	logTime := time.Unix(1382033155, 0)
	expected := writer.WriteRequest{
		Category: "login",
		Line:     "\t\t" + logTime.In(PST).Format(transformer.RedshiftDatetimeIngestString) + "\t",
		UUID:     "uuid1",
		Source:   nil,
		Failure:  reporter.SKIPPED_COLUMN,
		Pstart:   now,
	}

	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}

	p := BuildTestPool(1, 1, _parser, _transformer)
	p.Listen(w)
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()

	if len(w.requests) < 1 {
		t.Logf("expeceted 2 results")
		t.Fail()
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
		Line:     "0.1500000059604645\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t2013-10-17 11:05:55\t2013-09-30 17:00:02",
		UUID:     "uuid1",
		Source:   []byte(expectedJSONBytes),
		Pstart:   now,
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := BuildTestPool(1, 1, _parser, _transformer)
	p.Listen(w)
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) < 1 {
		t.Logf("expeceted 2 results")
		t.Fail()
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
		Failure:  reporter.UNABLE_TO_PARSE_DATA,
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := BuildTestPool(1, 1, _parser, _transformer)
	p.Listen(w)
	p.Process(_exampleRequest)

	time.Sleep(100 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) < 1 {
		t.Logf("expeceted 1 results")
		t.Fail()
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
	expected := []*writer.WriteRequest{
		&writer.WriteRequest{
			Category: "login",
			Line:     "0.1500000059604645\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t2013-10-17 11:05:55\t2013-09-30 17:00:02",
			UUID:     "uuid1-0",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		&writer.WriteRequest{
			Category: "login",
			Line:     "0.1500000059604645\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t2013-10-17 11:05:55\t2013-09-30 17:00:02",
			UUID:     "uuid1-1",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		&writer.WriteRequest{
			Category: "login",
			Line:     "0.1500000059604645\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t2013-10-17 11:05:55\t2013-09-30 17:00:02",
			UUID:     "uuid1-2",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
		&writer.WriteRequest{
			Category: "login",
			Line:     "0.1500000059604645\t\"FFFF8047-0398-40FF-FF89-5B3FFFFFF0E7\"\t2013-10-17 11:05:55\t2013-09-30 17:00:02",
			UUID:     "uuid1-3",
			Source:   []byte(expectedJSONBytes),
			Pstart:   now,
		},
	}
	w := &testWriter{
		m: &sync.Mutex{},

		requests: make([]*writer.WriteRequest, 0, 1),
	}
	p := BuildTestPool(5, 30, _parser, _transformer)
	p.Listen(w)
	p.Process(_exampleRequest)

	time.Sleep(500 * time.Millisecond) // Hopefully enough wait time...
	w.m.Lock()
	defer w.m.Unlock()
	if len(w.requests) != len(expected) {
		t.Logf("expeceted %d results got %d\n", len(expected), len(w.requests))
		t.Fail()
	}

	for i, e := range w.requests {
		if !requestEqual(expected[i], e) {
			fmt.Println(string(e.Source))
			t.Logf("Expected %+v but got %+v\n", expected[i], e)
			t.Fail()
		}

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

	rp := BuildTestPool(15, 30, _parser, _transformer)
	rp.Listen(w)

	b.ReportAllocs()
	b.ResetTimer()
	wait := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wait.Add(1)
		go func() {
			for j := 0; j < QUEUE_SIZE*2; j++ {
				<-w.r
			}
			wait.Done()
		}()
		for j := 0; j < QUEUE_SIZE*2; j++ {
			rp.Process(_exampleRequest)
		}
		wait.Wait()
	}
}
