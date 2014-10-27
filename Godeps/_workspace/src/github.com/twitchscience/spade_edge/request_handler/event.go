package request_handler

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

type EventRecord interface {
	AuditTrail() string
	HttpRequest() string
}

const (
	EVENT_VERSION = 1
)

type Event struct {
	ReceivedAt time.Time
	ClientIp   string
	UUID       string
	Data       string
	Version    int
}

func (e *Event) AuditTrail() string {
	return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.UUID)
}

func (e *Event) HttpRequest() string {
	// TODO: over time we probably want to move all values in a line to
	// being similar to "recordversion", i.e. they should use something
	// structured like logfmt / protobuf / etc
	b := bytes.NewBuffer(make([]byte, 0, 256))
	b.WriteString(e.ClientIp)
	b.WriteString(" [")
	b.WriteString(strconv.FormatInt(e.ReceivedAt.Unix(), 10))
	b.WriteString(".000] data=")
	b.WriteString(e.Data)
	b.WriteRune(' ')
	b.WriteString(e.UUID)
	b.WriteRune(' ')
	b.WriteString(fmt.Sprintf("%s=%d", "recordversion", e.Version))

	return b.String()
}
