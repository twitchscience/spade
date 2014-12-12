package nginx

import (
	"io/ioutil"
	"net/url"
	"testing"
	"github.com/twitchscience/spade/reporter"
)

var (
	sample        = []byte("eyJldmVudCI6ImhlbGxvIn0%3D")
	sampleLogLine = loadFile("test_resources/sample_data.txt")
)

func loadFile(file string) []byte {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}
	return b
}

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

func TestDecodeData(t *testing.T) {
	test1 := parseResult{
		ip:   "22.22.22.222",
		when: "1395707641",
		data: sampleLogLine,
		uuid: "39bffff7-4ffff880-539775b5-0",
	}
	p := &NginxLogParser{
		Escaper: &ByteQueryUnescaper{},
	}
	_, err := p.decodeData(&test1)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestBadUUIDDecodeData(t *testing.T) {
	test1 := parseResult{
		ip:   "22.22.22.222",
		when: "1395707641",
		data: []byte("ip=1"),
		uuid: "",
	}
	p := &NginxLogParser{
		Escaper: &ByteQueryUnescaper{},
	}
	_, err := p.decodeData(&test1)
	if err == nil {
		t.Fatalf("should have gotten error")
	}
}
