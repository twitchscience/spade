package request_handler

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twitchscience/aws_utils/environment"

	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/spade_edge/uuid"
	"github.com/cactus/go-statsd-client/statsd"
)

var (
	Assigner uuid.UUIDAssigner = uuid.StartUUIDAssigner(
		os.Getenv("HOST"),
		os.Getenv("CLOUD_CLUSTER"),
	)
	xDomainContents []byte = func() []byte {
		filename := os.Getenv("CROSS_DOMAIN_LOCATION")
		if filename == "" {
			filename = "../build/config/crossdomain.xml"
		}
		b, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatalln("Cross domain file not found: ", err)
		}
		return b
	}()
	xarth              []byte = []byte("XARTH")
	xmlApplicationType        = mime.TypeByExtension(".xml")
	DataFlag           []byte = []byte("data=")
	isProd                    = environment.IsProd()
)

type SpadeEdgeLogger interface {
	Log(string, string, time.Time, string) error
	Close()
}

type SpadeHandler struct {
	StatLogger statsd.Statter
	EdgeLogger SpadeEdgeLogger
	Assigner   uuid.UUIDAssigner
}

type FileAuditLogger struct {
	AuditLogger *gologging.UploadLogger
	SpadeLogger *gologging.UploadLogger
}

type Event struct {
	RecievedAt time.Time
	ClientIp   net.IP
	UUID       string
	Data       string
}

func (a *FileAuditLogger) Close() {
	a.AuditLogger.Close()
	a.SpadeLogger.Close()
}

func (a *FileAuditLogger) Log(ip string, data string, t time.Time, UUID string) error {
	a.AuditLogger.Log("[%d] %s", t.Unix(), UUID)
	a.SpadeLogger.Log("%s", writeInfo(ip, data, t, UUID))
	return nil
}

func writeInfo(ip string, data string, t time.Time, UUID string) string {
	b := bytes.NewBuffer(make([]byte, 0, 256))
	b.WriteString(ip)
	b.WriteString(" [")
	b.WriteString(strconv.FormatInt(t.Unix(), 10))
	b.WriteString(".000] data=")
	b.WriteString(data)
	b.WriteRune(' ')
	b.WriteString(UUID)
	return b.String()
}

func getIpFromHeader(headerKey string, header http.Header) string {
	clientIp := header.Get(headerKey)
	if clientIp == "" {
		return clientIp
	}
	comma := strings.Index(clientIp, ",")
	if comma > -1 {
		clientIp = clientIp[:comma]
	}

	return clientIp
}

func extractDataQuery(rawQuery string) string {
	dataIdx := strings.Index(rawQuery, "data=") + 5
	if dataIdx < 5 {
		return ""
	}
	endOfData := strings.IndexRune(rawQuery[dataIdx:], '&')
	if endOfData < 0 {
		return rawQuery[dataIdx:]
	}
	return rawQuery[dataIdx : dataIdx+endOfData]
}

func (s *SpadeHandler) HandleSpadeRequests(r *http.Request, context *requestContext) int {
	statTimer := newTimerInstance()
	switch r.Method {
	case "GET":
		clientIp := getIpFromHeader(context.IpHeader, r.Header)
		if clientIp == "" {
			return http.StatusBadRequest
		}
		context.Timers["ip"] = statTimer.stopTiming()

		statTimer.reset()
		// Get data from url
		queryString, err := url.QueryUnescape(r.URL.RawQuery)
		if err != nil {
			return http.StatusBadRequest
		}
		data := extractDataQuery(queryString)
		if data == "" {
			return http.StatusBadRequest
		}
		context.Timers["data"] = statTimer.stopTiming()

		// // get event
		statTimer.reset()
		uuid := s.Assigner.Assign()
		context.Timers["uuid"] = statTimer.stopTiming()

		statTimer.reset()
		s.EdgeLogger.Log(clientIp, data, context.Now, uuid)
		context.Timers["write"] = statTimer.stopTiming()

		return http.StatusNoContent
	case "POST":
		// Get client IP
		clientIp := getIpFromHeader(context.IpHeader, r.Header)
		if clientIp == "" {
			return http.StatusBadRequest
		}
		context.Timers["ip"] = statTimer.stopTiming()

		// This supports one request per post...
		statTimer.reset()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return http.StatusBadRequest
		}
		if bytes.Equal(b[:5], DataFlag) {
			b = b[5:]
		}
		context.Timers["data"] = statTimer.stopTiming()

		// get event
		statTimer.reset()
		uuid := s.Assigner.Assign()
		context.Timers["uuid"] = statTimer.stopTiming()

		statTimer.reset()
		s.EdgeLogger.Log(clientIp, string(b), context.Now, uuid)
		context.Timers["write"] = statTimer.stopTiming()

		return http.StatusNoContent
	default:
		return http.StatusBadRequest
	}
}

const (
	ipOverrideHeader = "X-Original-Ip"
	ipForwardHeader  = "X-Forwarded-For"
	badEndpoint      = "FourOhFour"
	nTimers          = 5
)

func getTimeStampFromHeader(r *http.Request) (time.Time, error) {
	timeStamp := r.Header.Get("X-ORIGINAL-MSEC")
	if timeStamp != "" {
		splitIdx := strings.Index(timeStamp, ".")
		if splitIdx > -1 {
			secs, err := strconv.ParseInt(timeStamp[:splitIdx], 10, 64)
			if err == nil {
				return time.Unix(secs, 0), nil
			}
		}
	}
	return time.Time{}, errors.New("could not process timestamp from header")
}

var allowedMethods = map[string]bool{
	"GET":  true,
	"POST": true,
}

func (s *SpadeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowedMethods[r.Method] {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	now := time.Now()
	var ts time.Time
	var err error
	ts = now
	// For integration time correction
	if !isProd {
		ts, err = getTimeStampFromHeader(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	//
	context := &requestContext{
		Now:      ts,
		Method:   r.Method,
		Endpoint: r.URL.Path,
		IpHeader: ipForwardHeader,
		Timers:   make(map[string]time.Duration, nTimers),
	}
	timer := newTimerInstance()
	context.setStatus(s.serve(w, r, context))
	context.Timers["http"] = timer.stopTiming()

	context.recordStats(s.StatLogger)
}

func (s *SpadeHandler) serve(w http.ResponseWriter, r *http.Request, context *requestContext) int {
	var status int
	switch r.URL.Path {
	case "/crossdomain.xml":
		w.Header().Add("Content-Type", xmlApplicationType)
		w.Write(xDomainContents)
		status = http.StatusOK
	case "/healthcheck":
		status = http.StatusOK
	case "/xarth":
		w.Write(xarth)
		status = http.StatusOK
	// Accepted tracking endpoints.
	case "/":
		status = s.HandleSpadeRequests(r, context)
	case "/track":
		status = s.HandleSpadeRequests(r, context)
	case "/track/":
		status = s.HandleSpadeRequests(r, context)
	// dont track everything else
	default:
		context.Endpoint = badEndpoint
		status = http.StatusNotFound
	}
	w.WriteHeader(status)
	return status
}
