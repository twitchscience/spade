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
	Endpoint string
	Verb     string
	Body     string
}

type testResponse struct {
	Code int
	Body string
}

type testTuple struct {
	Request  testRequest
	Response testResponse
}

var epoch = time.Unix(0, 0)

func (t *testEdgeLogger) Log(ip string, data string, m time.Time, UUID string) error {
	t.events = append(t.events, writeInfo(ip, data, epoch, UUID))
	return nil
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

func TestExtractDataQuery(t *testing.T) {
	for expected, input := range map[string]string{
		"blah":  "data=blah",
		"blah2": "data=blah2&",
	} {
		actual := extractDataQuery(input)
		if expected != actual {
			t.Fatalf("expected %s but got %s\n", expected, actual)
		}
	}
}

func TestEndPoints(t *testing.T) {
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
			"http://spade.twitch.tv/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		req.Header.Add("X-Original-Msec", "1399059241.000")
		SpadeHandler.ServeHTTP(testrecorder, req)
		if testrecorder.Code != tt.Response.Code {
			t.Fatalf("%s expected code %d not %d\n", tt.Request.Endpoint, tt.Response.Code, testrecorder.Code)
		}
		if testrecorder.Body.String() != tt.Response.Body {
			t.Fatalf("%s expected body %s not %s\n", tt.Request.Endpoint, tt.Response.Body, testrecorder.Body.String())
		}
	}
	// test logs
	expectedEvents := []string{
		"222.222.222.222 [0.000] data=blah 1",
		"222.222.222.222 [0.000] data=blah 2",
		"222.222.222.222 [0.000] data=eyJldmVudCI6ImhlbGxvIn0 3",
		"222.222.222.222 [0.000] data=blat 4",
		"222.222.222.222 [0.000] data=blag 5",
		"222.222.222.222 [0.000] data=bleck 6",
	}
	if !reflect.DeepEqual(SpadeHandler.EdgeLogger.(*testEdgeLogger).events, expectedEvents) {
		t.Fatalf("expected %s not %s\n", expectedEvents, SpadeHandler.EdgeLogger.(*testEdgeLogger).events)
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
			"http://spade.twitch.tv/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		req.Header.Add("X-Original-Msec", "1399059241.000")
		context := &requestContext{
			Now:      time.Now(),
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

func BenchmarkExtractDataQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		extractDataQuery("data=1234567890101112131415161718191011239601278364o8123746&")
	}
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
			Request: testRequest{
				Endpoint: "track?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "track/?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "track/?data=eyJldmVudCI6ImhlbGxvIn0%26ip%3D1",
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
			Request: testRequest{
				Endpoint: "?data=blat",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Verb: "POST",
				Body: "blag",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Verb: "POST",
				Body: "data=bleck",
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
