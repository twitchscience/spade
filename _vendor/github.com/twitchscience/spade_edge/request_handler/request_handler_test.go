package request_handler

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/scoop_protocol/spade"
)

type testEdgeLogger struct {
	events [][]byte
}

type testUUIDAssigner struct {
	i int
}

type testRequest struct {
	Endpoint    string
	Verb        string
	ContentType string
	Body        string
}

type testResponse struct {
	Code int
	Body string
}

type testTuple struct {
	Expectation string
	Request     testRequest
	Response    testResponse
}

var epoch = time.Unix(0, 0)

func (t *testEdgeLogger) Log(e *spade.Event) error {
	logLine, err := spade.Marshal(e)
	if err != nil {
		return err
	}
	t.events = append(t.events, logLine)
	return nil
}

func (t *testEdgeLogger) Close() {}

func (t *testUUIDAssigner) Assign() string {
	t.i++
	return fmt.Sprintf("%d", t.i)
}

func TestParseLastForwarder(t *testing.T) {
	var testHeaders = []struct {
		input    string
		expected net.IP
	}{
		{"a, b, 192.168.1.1", net.ParseIP("192.168.1.1")},
		{"a, b,192.168.1.1 ", net.ParseIP("192.168.1.1")},
		{"a, 10.1.1.1,", nil},
		{" 192.168.1.1", net.ParseIP("192.168.1.1")},
	}

	for _, h := range testHeaders {
		output := parseLastForwarder(h.input)
		if !h.expected.Equal(output) {
			t.Fatalf("%s -> %s instead of expected %s", h.input, output, h.expected)
		}
	}
}

var fixedTime = time.Date(2014, 5, 2, 19, 34, 1, 0, time.UTC)

func makeSpadeHandler() *SpadeHandler {
	c, _ := statsd.NewNoop()
	SpadeHandler := NewSpadeHandler(c, &testEdgeLogger{}, &testUUIDAssigner{}, "")
	SpadeHandler.Time = func() time.Time { return fixedTime }
	return SpadeHandler
}

func TestEndPoints(t *testing.T) {
	SpadeHandler := makeSpadeHandler()
	var expectedEvents []spade.Event
	fixedIP := net.ParseIP("222.222.222.222")

	uuidCounter := 1

	for _, tt := range testRequests {
		testrecorder := httptest.NewRecorder()
		req, err := http.NewRequest(
			tt.Request.Verb,
			"http://spade.twitch.tv/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		if tt.Request.ContentType != "" {
			req.Header.Add("Content-Type", tt.Request.ContentType)
		}
		SpadeHandler.ServeHTTP(testrecorder, req)
		if testrecorder.Code != tt.Response.Code {
			t.Fatalf("%s expected code %d not %d\n", tt.Request.Endpoint, tt.Response.Code, testrecorder.Code)
		}
		if testrecorder.Body.String() != tt.Response.Body {
			t.Fatalf("%s expected body %s not %s\n", tt.Request.Endpoint, tt.Response.Body, testrecorder.Body.String())
		}

		if tt.Expectation != "" {
			expectedEvents = append(expectedEvents, spade.Event{
				ReceivedAt:    fixedTime.UTC(),
				ClientIp:      fixedIP,
				XForwardedFor: fixedIP.String(),
				Uuid:          fmt.Sprintf("%d", uuidCounter),
				Data:          tt.Expectation,
				Version:       spade.PROTOCOL_VERSION,
			})
			uuidCounter++
		}
	}
	for idx, byteLog := range SpadeHandler.EdgeLogger.(*testEdgeLogger).events {
		var ev spade.Event
		err := spade.Unmarshal(byteLog, &ev)
		if err != nil {
			t.Errorf("Expected Unmarshal to work, input: %s, err: %s", byteLog, err)
		}
		if !reflect.DeepEqual(ev, expectedEvents[idx]) {
			t.Errorf("Event processed incorrectly: expected: %v got: %v", expectedEvents[idx], ev)
		}
	}
}

func TestHandle(t *testing.T) {
	SpadeHandler := makeSpadeHandler()
	for _, tt := range testRequests {
		testrecorder := httptest.NewRecorder()
		req, err := http.NewRequest(
			tt.Request.Verb,
			"http://spade.example.com/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		if tt.Request.ContentType != "" {
			req.Header.Add("Content-Type", tt.Request.ContentType)
		}
		context := &requestContext{
			Now:      epoch,
			Method:   req.Method,
			Endpoint: req.URL.Path,
			IpHeader: ipForwardHeader,
			Timers:   make(map[string]time.Duration, nTimers),
		}
		status := SpadeHandler.serve(testrecorder, req, context)

		if status != tt.Response.Code {
			t.Fatalf("%s expected code %d not %d\n", tt.Request.Endpoint, tt.Response.Code, testrecorder.Code)
		}
	}
}

func isEndpointGood(endpoint string) bool {
	idx := strings.Index(endpoint, "?")
	if idx > -1 {
		endpoint = endpoint[:idx]
	}
	endpoint = "/" + endpoint
	for _, valid := range goodEndpoints {
		if endpoint == valid {
			return true
		}
	}
	return false
}

func BenchmarkUUIDAssigner(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Assigner.Assign()
	}
	b.ReportAllocs()

}

func BenchmarkRequests(b *testing.B) {
	SpadeHandler := makeSpadeHandler()
	reqGet, err := http.NewRequest("GET", "http://spade.twitch.tv/?data=blah", nil)
	if err != nil {
		b.Fatalf("Failed to build request error: %s\n", err)
	}
	reqGet.Header.Add("X-Forwarded-For", "222.222.222.222")

	reqPost, err := http.NewRequest("POST", "http://spade.twitch.tv/", strings.NewReader("data=blah"))
	if err != nil {
		b.Fatalf("Failed to build request error: %s\n", err)
	}
	reqPost.Header.Add("X-Forwarded-For", "222.222.222.222")
	testrecorder := httptest.NewRecorder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			SpadeHandler.ServeHTTP(testrecorder, reqPost)
		} else {
			SpadeHandler.ServeHTTP(testrecorder, reqGet)
		}
	}
	b.ReportAllocs()
}

var (
	testRequests = []testTuple{
		testTuple{
			Request: testRequest{
				Endpoint: "crossdomain.xml",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusOK,
				Body: string(xDomainContents),
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "healthcheck",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusOK,
			},
		},
		testTuple{
			Expectation: "blah",
			Request: testRequest{
				Endpoint: "track?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "blah",
			Request: testRequest{
				Endpoint: "track/?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "eyJldmVudCI6ImhlbGxvIn0",
			Request: testRequest{
				Endpoint: "track/?data=eyJldmVudCI6ImhlbGxvIn0&ip=1",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "track",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusBadRequest,
			},
		},
		testTuple{
			Expectation: "blat",
			Request: testRequest{
				Endpoint: "?data=blat",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "blag",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-randomfoofoo",
				Body:        "blag",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		// The next request is a bad client that passes our tests,
		// hopefully these should be incredibly rare. They do not parse at
		// our processor level
		testTuple{
			Expectation: "ip=&data=blagi",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-randomfoofoo",
				Body:        "ip=&data=blagi",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "bleck",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-www-form-urlencoded",
				Body:        "data=bleck",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "blog",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-www-form-urlencoded",
				Body:        "ip=&data=blog",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "blem",
			Request: testRequest{
				Verb:     "POST",
				Endpoint: "track?ip=&data=blem",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Expectation: "blamo",
			Request: testRequest{
				Verb:     "GET",
				Endpoint: "track?ip=&data=blamo",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "/spam/spam",
				Verb:     "POST",
				Body:     "data=bleck",
			},
			Response: testResponse{
				Code: http.StatusNotFound,
			},
		},
	}
	goodEndpoints = []string{
		"/crossdomain.xml",
		"/healthcheck",
		"/xarth",
		"/integration",
		"/",
		"/track",
		"/track/",
	}
)
