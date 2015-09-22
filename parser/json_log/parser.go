package json_log

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

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
	rejectIfBadFirstIp bool // Test for whether the previous edge would've rejected the event and replicate that behavior
}

func Register(rejectIfBadFirstIp bool) {
	parser.Register("json_log", &jsonLogParser{
		rejectIfBadFirstIp: rejectIfBadFirstIp,
	})
	if rejectIfBadFirstIp == true {
		log.Println("Warning: Rejecting any event with a bad first ip in the x-forwarded chain")
	}
}

// Return true if this would have passed the original IP test on the edge (for version 3 events)
func wasValidEdgeIp(xForwardedFor string) bool {
	var clientIp string
	comma := strings.Index(xForwardedFor, ",")
	if comma > -1 {
		clientIp = xForwardedFor[:comma]
	} else {
		clientIp = xForwardedFor
	}
	return net.ParseIP(clientIp) != nil
}

func (j *jsonLogParser) Parse(raw parser.Parseable) ([]parser.MixpanelEvent, error) {
	var rawEvent spade.Event
	err := spade.Unmarshal(raw.Data(), &rawEvent)
	if err != nil {
		return []parser.MixpanelEvent{*parser.MakeErrorEvent(raw, "", "")}, err
	}

	if j.rejectIfBadFirstIp && rawEvent.Version == 3 && !wasValidEdgeIp(rawEvent.XForwardedFor) {
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
		m[i].ClientIp = rawEvent.ClientIp.String()
		m[i].Pstart = raw.StartTime()
		if len(events) > 1 {
			m[i].UUID = fmt.Sprintf("%s-%d", parsedEvent.UUID(), i)
		} else {
			m[i].UUID = parsedEvent.UUID()
		}
	}
	return m, nil
}
