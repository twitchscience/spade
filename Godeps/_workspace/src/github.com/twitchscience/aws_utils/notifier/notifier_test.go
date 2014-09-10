package notifier

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

func TestMessageRegister(t *testing.T) {
	DefaultClient.Signer.RegisterMessageType("test", func(args ...interface{}) (string, error) {
		if len(args) < 3 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"version\":%d,\"keyname\":%q,\"size\":%d}", args...), nil
	})
	expected := "{\"version\":0,\"keyname\":\"TestKey\",\"size\":500}"
	actual, _ := DefaultClient.Signer.SignBody("test", 0, "TestKey", 500)
	if actual != expected {
		t.Logf("expected %s but got %s", expected, actual)
		t.Fail()
	}
	actual, err := DefaultClient.Signer.SignBody("test", 0, "TestKey")
	if err == nil {
		t.Logf("expected %s but got %s", expected, actual)
		t.Fail()
	}
}

func assert(test bool, t *testing.T) {
	if !test {
		t.FailNow()
	}
}

func TestHTTPClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skippped test in short mode")
	}
	(&HTTPSuite{}).SetUpSuite()

	auth := aws.Auth{AccessKey: "abc", SecretKey: "123"}
	region := aws.Region{SQSEndpoint: testServer.URL}
	sqsClient := BuildSQSClient(auth, region)
	q := &sqs.Queue{
		SQS: sqsClient.SQS, Url: testServer.URL + "/123456789012/testQueue/"}
	testServer.PrepareResponse(200, nil, TestSendMessageXmlOK, 11*time.Second)
	testServer.PrepareResponse(200, nil, TestSendMessageXmlOK, 11*time.Second)
	testServer.PrepareResponse(200, nil, TestSendMessageXmlOK, 11*time.Second)

	now := time.Now()
	err := sqsClient.handle("test", q)
	// Should take (1 + 2) + (1 + 4) + (1 + 8) = 17 secs. We subtract two seconds here for safety.
	assert(time.Now().Sub(now) > 40*time.Second, t)
	assert(err != nil, t)
}

func TestClient(t *testing.T) {
	(&HTTPSuite{}).SetUpSuite()
	auth := aws.Auth{AccessKey: "abc", SecretKey: "123"}
	region := aws.Region{SQSEndpoint: testServer.URL}
	sqsClient := BuildSQSClient(auth, region)
	q := &sqs.Queue{SQS: sqsClient.SQS, Url: testServer.URL + "/123456789012/testQueue/"}

	testServer.PrepareResponse(200, nil, TestSendMessageXmlOK, time.Duration(0))

	err := sqsClient.handle("test", q)

	req := testServer.WaitRequest()
	assert(req.Method == "GET", t)
	assert(req.URL.Path == "/123456789012/testQueue/", t)
	assert(err == nil, t)
}

var TestSendMessageXmlOK = `
<SendMessageResponse>
  <SendMessageResult>
    <MD5OfMessageBody>098f6bcd4621d373cade4e832627b4f6</MD5OfMessageBody>
    <MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId>
  </SendMessageResult>
  <ResponseMetadata>
    <RequestId>27daac76-34dd-47df-bd01-1f6e873584a0</RequestId>
  </ResponseMetadata>
</SendMessageResponse>
`

type HTTPSuite struct{}

var testServer = NewTestHTTPServer("http://localhost:4455", 5e9)

func (s *HTTPSuite) SetUpSuite() {
	testServer.Start()
}

func (s *HTTPSuite) TearDownTest() {
	testServer.FlushRequests()
}

type TestHTTPServer struct {
	URL      string
	Timeout  time.Duration
	started  bool
	request  chan *http.Request
	response chan *testResponse
	pending  chan bool
}

type testResponse struct {
	Status  int
	Headers map[string]string
	Body    string
	Wait    time.Duration
}

func NewTestHTTPServer(url string, timeout time.Duration) *TestHTTPServer {
	return &TestHTTPServer{URL: url, Timeout: timeout}
}

func (s *TestHTTPServer) Start() {
	if s.started {
		return
	}
	s.started = true

	s.request = make(chan *http.Request, 64)
	s.response = make(chan *testResponse, 64)
	s.pending = make(chan bool, 64)

	url, _ := url.Parse(s.URL)
	go func() {
		err := http.ListenAndServe(url.Host, s)
		if err != nil {
			panic(err)
		}
	}()

	s.PrepareResponse(202, nil, "Nothing.", 0)
	for {
		// Wait for it to be up.
		resp, err := http.Get(s.URL)
		if err == nil && resp.StatusCode == 202 {
			break
		}
		fmt.Fprintf(os.Stderr, "\nWaiting for fake server to be up... ")
		time.Sleep(1e8)
	}
	s.WaitRequest() // Consume dummy request.
}

// FlushRequests discards requests which were not yet consumed by WaitRequest.
func (s *TestHTTPServer) FlushRequests() {
	for {
		select {
		case <-s.request:
		default:
			return
		}
	}
}

func (s *TestHTTPServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.request <- req
	var resp *testResponse
	select {
	case resp = <-s.response:
	case <-time.After(s.Timeout):
		fmt.Fprintf(os.Stderr, "ERROR: Timeout waiting for test to provide response\n")
		resp = &testResponse{500, nil, "", 0}
	}
	time.Sleep(resp.Wait)
	if resp.Headers != nil {
		h := w.Header()
		for k, v := range resp.Headers {
			h.Set(k, v)
		}
	}
	if resp.Status != 0 {
		w.WriteHeader(resp.Status)
	}
	w.Write([]byte(resp.Body))
}

func (s *TestHTTPServer) WaitRequest() *http.Request {
	select {
	case req := <-s.request:
		req.ParseForm()
		return req
	case <-time.After(s.Timeout):
		panic("Timeout waiting for goamz request")
	}
}

func (s *TestHTTPServer) PrepareResponse(status int, headers map[string]string, body string, wait time.Duration) {
	s.response <- &testResponse{status, headers, body, wait}
}
