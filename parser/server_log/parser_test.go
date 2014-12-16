package server_log

import (
	"net/url"
	"testing"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

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
	escaper := parser.ByteQueryUnescaper{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		escaper.QueryUnescape(sample)
	}
}
