package parser

import (
	"io/ioutil"
	"net/url"
	"testing"
)

func loadFile(file string) []byte {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}
	return b
}

func TestDecodeData(t *testing.T) {
	tcs := []struct {
		data        []byte
		shouldError bool
	}{
		{loadFile("test_resources/b64payload.txt"), false},
		{loadFile("test_resources/b64payload_space.txt"), false},
		{loadFile("test_resources/urlsafeb64payload.txt"), false},
		{[]byte("ip=1"), true},
	}
	for _, tc := range tcs {
		_, err := DecodeBase64(tc.data, &ByteQueryUnescaper{})
		if tc.shouldError && err == nil {
			t.Fatalf("got error: %v\n", err)
		} else if !tc.shouldError && err != nil {
			t.Fatalf("should have gotten error")
		}
	}
}

func TestSpaceDecodeData(t *testing.T) {
	_, err := DecodeBase64(loadFile("test_resources/b64payload_space.txt"), &ByteQueryUnescaper{})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestURLSafeDecodeData(t *testing.T) {
	_, err := DecodeBase64(loadFile("test_resources/urlsafeb64payload.txt"), &ByteQueryUnescaper{})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestBadUUIDDecodeData(t *testing.T) {
	_, err := DecodeBase64([]byte("ip=1"), &ByteQueryUnescaper{})
	if err == nil {
	}
}

var sample = []byte("eyJldmVudCI6ImhlbGxvIn0%3D")

type StringQueryUnescaper struct{}

func (s *StringQueryUnescaper) QueryUnescape(q []byte) ([]byte, error) {
	out, err := url.QueryUnescape(string(q))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

func BenchmarkStringQueryUnescape(b *testing.B) {
	escaper := StringQueryUnescaper{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = escaper.QueryUnescape(sample)
	}
}

func BenchmarkByteQueryUnescape(b *testing.B) {
	escaper := ByteQueryUnescaper{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = escaper.QueryUnescape(sample)
	}
}
