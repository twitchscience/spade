package json

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade/parser"
)

type jsonLogEvent struct {
	event spade.Event
}

func (j *jsonLogEvent) Data() []byte {
	return []byte(j.event.Data)
}

func (j *jsonLogEvent) UUID() string {
	return j.event.Uuid
}

func (j *jsonLogEvent) Time() string {
	return fmt.Sprintf("%d", j.event.ReceivedAt.Unix())
}

type jsonLogParser struct {
	rejectIfBadFirstIP bool // Test for whether the previous edge would've rejected the event and replicate that behavior
}

// Register registers a json_log parser in the global list of parsers.
func Register(rejectIfBadFirstIP bool) error {
	err := parser.Register("json_log", &jsonLogParser{
		rejectIfBadFirstIP: rejectIfBadFirstIP,
	})
	if err != nil {
		return err
	}
	if rejectIfBadFirstIP {
		logger.Warn("Rejecting any event with a bad first IP in the x-forwarded chain")
	}
	return nil
}

// wasValidEdgeIP returns true if this would have passed the original IP test
// on the edge (for version 3 events)
func wasValidEdgeIP(xForwardedFor string) bool {
	var clientIP string
	comma := strings.Index(xForwardedFor, ",")
	if comma > -1 {
		clientIP = xForwardedFor[:comma]
	} else {
		clientIP = xForwardedFor
	}
	return net.ParseIP(clientIP) != nil
}

func (j *jsonLogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	var rawEvent spade.Event
	err := spade.Unmarshal(raw.Data(), &rawEvent)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, "", "")}, err
	}

	if j.rejectIfBadFirstIP && rawEvent.Version == 3 && !wasValidEdgeIP(rawEvent.XForwardedFor) {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, rawEvent.Uuid, strconv.FormatInt(rawEvent.ReceivedAt.Unix(), 10))},
			fmt.Errorf("Event uuid %s had invalid first client IP", rawEvent.Uuid)
	}

	parsedEvent := &jsonLogEvent{event: rawEvent}
	events, err := parser.DecodeBase64(parsedEvent, &parser.ByteQueryUnescaper{})
	if err != nil {
		return []parser.MixpanelEvent{
			*parser.MakeErrorEvent(raw, rawEvent.Uuid, parsedEvent.Time()),
		}, err
	}

	m := make([]parser.MixpanelEvent, len(events))
	for i, e := range events {
		m[i] = e
		m[i].EventTime = json.Number(parsedEvent.Time())
		m[i].ClientIP = rawEvent.ClientIp.String()
		m[i].Pstart = raw.StartTime()
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", parsedEvent.UUID(), i)
		} else {
			m[i].UUID = parsedEvent.UUID()
		}
	}
	return m, nil
}
