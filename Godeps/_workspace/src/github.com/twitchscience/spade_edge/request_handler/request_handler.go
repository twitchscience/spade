package request_handler

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/uuid"
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
)

const corsMaxAge = "86400" // One day

type SpadeEdgeLogger interface {
	Log(event *spade.Event) error
	Close()
}

type NoopLogger struct{}

func (n *NoopLogger) Log(e *spade.Event) error { return nil }
func (n *NoopLogger) Close()                   {}

type SpadeHandler struct {
	StatLogger  statsd.Statter
	EdgeLogger  SpadeEdgeLogger
	Assigner    uuid.UUIDAssigner
	Time        func() time.Time // Defaults to time.Now
	corsOrigins map[string]bool
}

type EventLoggers struct {
	AuditLogger *gologging.UploadLogger
	SpadeLogger *gologging.UploadLogger
	KLogger     SpadeEdgeLogger
}

func NewSpadeHandler(stats statsd.Statter, logger SpadeEdgeLogger, assigner uuid.UUIDAssigner, CORSOrigins string) *SpadeHandler {
	h := &SpadeHandler{
		StatLogger:  stats,
		EdgeLogger:  logger,
		Assigner:    assigner,
		Time:        time.Now,
		corsOrigins: make(map[string]bool),
	}

	origins := strings.Split(strings.TrimSpace(CORSOrigins), " ")
	for _, origin := range origins {
		trimmedOrigin := strings.TrimSpace(origin)
		if trimmedOrigin != "" {
			h.corsOrigins[trimmedOrigin] = true
		}
	}
	return h
}

func (a *EventLoggers) Init() {}

func (a *EventLoggers) Close() {
	a.AuditLogger.Close()
	a.SpadeLogger.Close()
	a.KLogger.Close()
}

func (a *EventLoggers) Log(event *spade.Event) error {
	a.AuditLogger.Log("%s", auditTrail(event))

	logLine, err := spade.Marshal(event)
	if err != nil {
		return err
	}
	a.SpadeLogger.Log("%s", logLine)
	a.KLogger.Log(event)
	return nil
}

func auditTrail(e *spade.Event) string {
	return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.Uuid)
}

func parseLastForwarder(header string) net.IP {
	var clientIp string
	comma := strings.LastIndex(header, ",")
	if comma > -1 && comma < len(header)+1 {
		clientIp = header[comma+1:]
	} else {
		clientIp = header
	}

	return net.ParseIP(strings.TrimSpace(clientIp))
}

func (s *SpadeHandler) HandleSpadeRequests(r *http.Request, context *requestContext) int {
	statTimer := newTimerInstance()

	xForwardedFor := r.Header.Get(context.IpHeader)
	clientIp := parseLastForwarder(xForwardedFor)

	context.Timers["ip"] = statTimer.stopTiming()

	err := r.ParseForm()
	if err != nil {
		return http.StatusBadRequest
	}

	data := r.Form.Get("data")
	if data == "" && r.Method == "POST" {
		// if we're here then our clients have POSTed us something weird,
		// for example, something that maybe
		// application/x-www-form-urlencoded but with the Content-Type
		// header set incorrectly... best effort here on out

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return http.StatusBadRequest
		}
		if bytes.Equal(b[:5], DataFlag) {
			context.BadClient = true
			b = b[5:]
		}
		data = string(b)

	}
	if data == "" {
		return http.StatusBadRequest
	}

	context.Timers["data"] = statTimer.stopTiming()

	// // get event
	uuid := s.Assigner.Assign()
	context.Timers["uuid"] = statTimer.stopTiming()

	event := spade.NewEvent(
		context.Now,
		clientIp,
		xForwardedFor,
		uuid,
		data,
	)

	// Note, Log() has a side effect of writing to the log
	if err = s.EdgeLogger.Log(event); err != nil {
		return http.StatusBadRequest
	}
	context.Timers["write"] = statTimer.stopTiming()

	return http.StatusNoContent
}

const (
	ipOverrideHeader = "X-Original-Ip"
	ipForwardHeader  = "X-Forwarded-For"
	badEndpoint      = "FourOhFour"
	nTimers          = 5
)

var allowedMethods = map[string]bool{
	"GET":     true,
	"POST":    true,
	"OPTIONS": true,
}
var allowedMethodsHeader string // Comma-separated version of allowedMethods

func (s *SpadeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowedMethods[r.Method] {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Vary", "Origin")

	origin := r.Header.Get("Origin")
	if s.corsOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", allowedMethodsHeader)
	}

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Max-Age", corsMaxAge)
		w.WriteHeader(http.StatusOK)
		return
	}

	context := &requestContext{
		Now:       s.Time(),
		Method:    r.Method,
		Endpoint:  r.URL.Path,
		IpHeader:  ipForwardHeader,
		Timers:    make(map[string]time.Duration, nTimers),
		BadClient: false,
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
		return http.StatusOK
	case "/healthcheck":
		status = http.StatusOK
	case "/xarth":
		w.Write(xarth)
		return http.StatusOK
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

func init() {
	var allowedMethodsList []string
	for k := range allowedMethods {
		allowedMethodsList = append(allowedMethodsList, k)
	}
	allowedMethodsHeader = strings.Join(allowedMethodsList, ", ")
}
