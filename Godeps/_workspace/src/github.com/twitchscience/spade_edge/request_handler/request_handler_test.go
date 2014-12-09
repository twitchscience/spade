package request_handler

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

type testEdgeLogger struct {
	events []string
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

func (t *testEdgeLogger) Log(record EventRecord) {
	t.events = append(t.events, record.HttpRequest())
}

func (t *testEdgeLogger) Close() {}

func (t *testUUIDAssigner) Assign() string {
	t.i++
	return fmt.Sprintf("%d", t.i)
}

func TestGetIpAndTimeStamp(t *testing.T) {
	testIp := net.ParseIP("222.222.222.222")
	testTime := time.Unix(1397768380, 0)

	headers := map[string][]string{
		textproto.CanonicalMIMEHeaderKey("X-ORIGINAL-MSEC"): []string{fmt.Sprintf("%d.000", testTime.Unix())},
		textproto.CanonicalMIMEHeaderKey("X-Forwarded-For"): []string{testIp.String()},
	}
	ip := getIpFromHeader("X-Forwarded-For", headers)

	if testIp.String() != ip {
		t.Errorf("Expecting %s for ip got %s\n", testIp, ip)
	}
}

func TestGetForwardedIpAndTimeStamp(t *testing.T) {
	testIp := net.ParseIP("222.222.222.222")
	testTime := time.Unix(1397768380, 0)

	headers := map[string][]string{
		textproto.CanonicalMIMEHeaderKey("X-ORIGINAL-MSEC"): []string{fmt.Sprintf("%d.000", testTime.Unix())},
		textproto.CanonicalMIMEHeaderKey("X-Forwarded-For"): []string{"222.222.222.222, 123.123.123.123, 123.123.123.124"},
	}
	ip := getIpFromHeader("X-Forwarded-For", headers)
	if testIp.String() != ip {
		t.Errorf("Expecting %s for ip got %s\n", testIp, ip)
	}
}

func TestEndPoints(t *testing.T) {
	c, _ := statsd.NewNoop()
	SpadeHandler := &SpadeHandler{
		EdgeLogger: &testEdgeLogger{},
		Assigner:   &testUUIDAssigner{},
		StatLogger: c,
	}

	var expectedEvents []string
	uuidIdx := 1
	baseFmt := "222.222.222.222 [1399059241.000] data=%s %d recordversion=1"

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
		req.Header.Add("X-Original-Msec", "1399059241.000")
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
			expectedEvents = append(expectedEvents, fmt.Sprintf(baseFmt, tt.Expectation, uuidIdx))
			uuidIdx++
		}
	}
	if !reflect.DeepEqual(SpadeHandler.EdgeLogger.(*testEdgeLogger).events, expectedEvents) {
		t.Fatalf(fmt.Sprintf("\nExpected: %s\nGot: %s", expectedEvents, SpadeHandler.EdgeLogger.(*testEdgeLogger).events))
	}
}

func TestHandle(t *testing.T) {
	c, _ := statsd.NewNoop()
	SpadeHandler := &SpadeHandler{
		EdgeLogger: &testEdgeLogger{},
		Assigner:   &testUUIDAssigner{},
		StatLogger: c,
	}
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
		req.Header.Add("X-Original-Msec", "1399059241.000")
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
	c, _ := statsd.NewNoop()
	SpadeHandler := &SpadeHandler{
		EdgeLogger: &testEdgeLogger{},
		Assigner:   Assigner,
		StatLogger: c,
	}
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
	isProd = true
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
