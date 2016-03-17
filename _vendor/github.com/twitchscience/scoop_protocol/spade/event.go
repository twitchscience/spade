package spade

import (
	"encoding/json"
	"net"
	"time"
)

// This might be a really bad idea, perhaps the version should be
// defined in the clients of this code, as it current stands we cannot
// move one without the other. Recommended ways to solve this sort of
// thing in Protobuf and Thrift is to have your namespace dicate version
const PROTOCOL_VERSION = 3

type Event struct {
	ReceivedAt    time.Time `json:"receivedAt"`
	ClientIp      net.IP    `json:"clientIp"`
	XForwardedFor string    `json:"xForwardedFor"`
	Uuid          string    `json:"uuid"`
	Data          string    `json:"data"`
	Version       int       `json:"recordversion"`
}

func NewEvent(receivedAt time.Time, clientIp net.IP, xForwardedFor, uuid, data string) *Event {
	return &Event{
		ReceivedAt:    receivedAt,
		ClientIp:      clientIp,
		XForwardedFor: xForwardedFor,
		Uuid:          uuid,
		Data:          data,
		Version:       PROTOCOL_VERSION,
	}
}

func Marshal(src *Event) ([]byte, error) {
	return json.Marshal(src)
}

func Unmarshal(b []byte, dst *Event) error {
	return json.Unmarshal(b, &dst)
}
