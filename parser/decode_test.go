package parser

import (
	"io/ioutil"
	"net/url"
	"testing"
	"github.com/twitchscience/spade/reporter"
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

var sample = []byte("eyJldmVudCI6ImhlbGxvIn0%3D")

type StringQueryUnescaper struct{}

func (s *StringQueryUnescaper) QueryUnescape(q []byte) ([]byte, error) {
	out, err := url.QueryUnescape(string(q))
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

type dummyReporter struct{}

func (d *dummyReporter) Record(c *reporter.Result) {}
func (d *dummyReporter) IncrementExpected(n int)   {}
func (d *dummyReporter) Reset()                    {}
func (d *dummyReporter) Finalize() map[string]int {
	return make(map[string]int)
}

func BenchmarkStringQueryUnescape(b *testing.B) {
	escaper := StringQueryUnescaper{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		escaper.QueryUnescape(sample)
	}
}

func BenchmarkByteQueryUnescape(b *testing.B) {
	escaper := ByteQueryUnescaper{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		escaper.QueryUnescape(sample)
	}
}
