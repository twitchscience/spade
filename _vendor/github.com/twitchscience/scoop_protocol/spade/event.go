package spade

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

// This might be a really bad idea, perhaps the version should be
// defined in the clients of this code, as it current stands we cannot
// move one without the other. Recommended ways to solve this sort of
// thing in Protobuf and Thrift is to have your namespace dicate version
const PROTOCOL_VERSION = 3
const COMPRESSION_VERSION byte = 0

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

func Compress(e *Event) ([]byte, error) {
	data, err := Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("Unable to marshal spade event: %s", err)
	}

	var compressed bytes.Buffer
	compressed.WriteByte(COMPRESSION_VERSION)
	flator, _ := flate.NewWriter(&compressed, flate.BestCompression)
	_, err = flator.Write(data)
	if err != nil {
		return nil, fmt.Errorf("Error writing to flator: %s", err)
	}

	err = flator.Close()
	if err != nil {
		return nil, fmt.Errorf("Error closing flator: %s", err)
	}

	return compressed.Bytes(), nil
}

func Decompress(c []byte) (*Event, error) {
	compressed := bytes.NewBuffer(c)
	v, err := compressed.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Error reading version byte: %s", err)
	}
	if v != COMPRESSION_VERSION {
		return nil, fmt.Errorf("Unknown version, got %v expected %v", v, COMPRESSION_VERSION)
	}

	deflator := flate.NewReader(compressed)

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, deflator)
	if err != nil {
		return nil, fmt.Errorf("Error decompressing event: %s", err)
	}
	err = deflator.Close()
	if err != nil {
		return nil, fmt.Errorf("Error decompressing event: %s", err)
	}

	e := &Event{}
	err = Unmarshal(decompressed.Bytes(), e)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling event %s", err)
	}

	return e, nil
}
