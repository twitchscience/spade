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
const PROTOCOL_VERSION = 4
const COMPRESSION_VERSION byte = 1
const INTERNAL_EDGE = "internal"
const EXTERNAL_EDGE = "external"

type Event struct {
	ReceivedAt    time.Time `json:"receivedAt"`
	ClientIp      net.IP    `json:"clientIp"`
	XForwardedFor string    `json:"xForwardedFor"`
	Uuid          string    `json:"uuid"`
	Data          string    `json:"data"`
	UserAgent     string    `json:"userAgent"`
	Version       int       `json:"recordversion"`
	EdgeType      string    `json:"edgeType"`
}

func NewEvent(receivedAt time.Time, clientIp net.IP, xForwardedFor, uuid, data, userAgent string, edgeType string) *Event {
	return &Event{
		ReceivedAt:    receivedAt,
		ClientIp:      clientIp,
		XForwardedFor: xForwardedFor,
		Uuid:          uuid,
		Data:          data,
		UserAgent:     userAgent,
		Version:       PROTOCOL_VERSION,
		EdgeType:      edgeType,
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

func Deglob(glob []byte) (events []*Event, err error) {
	compressed := bytes.NewBuffer(glob)

	v, err := compressed.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("error reading version byte: %s", err)
	}
	if v != COMPRESSION_VERSION {
		return nil, fmt.Errorf("unknown version: got %v expected %v", v, COMPRESSION_VERSION)
	}

	deflator := flate.NewReader(compressed)
	defer func() {
		if cerr := deflator.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("error closing glob reader: %v", cerr)
		}
	}()

	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, deflator)
	if err != nil {
		return nil, fmt.Errorf("error decompressing: %v", err)
	}

	err = json.Unmarshal(decompressed.Bytes(), &events)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling: %v", err)
	}

	return
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
