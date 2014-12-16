package parser

import (
	"io/ioutil"
	"testing"
)

func loadFile(file string) []byte {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}
	return b
}

type testShim struct {
	data []byte
	uuid string
}

func (t *testShim) Data() []byte {
	return t.data
}

func (t *testShim) UUID() string {
	return t.uuid
}

func (t *testShim) Time() string {
	return "1395707641"
}

func TestDecodeData(t *testing.T) {
	_, err := DecodeBase64(&testShim{
		data: loadFile("test_resources/b64payload.txt"),
		uuid: "39bffff7-4ffff880-539775b5-0",
	}, &ByteQueryUnescaper{})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestBadUUIDDecodeData(t *testing.T) {
	_, err := DecodeBase64(&testShim{
		data: []byte("ip=1"),
		uuid: "",
	}, &ByteQueryUnescaper{})
	if err == nil {
		t.Fatalf("should have gotten error")
	}
}
