package parser

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/TwitchScience/spade/reporter"
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
		Ip:   "22.22.22.222",
		Time: "1395707641",
		Data: sampleLogLine,
		UUID: "39bffff7-4ffff880-539775b5-0",
	}
	p := BuildSpadeParser(&dummyReporter{}).(*NginxLogParser)
	es, err := p.decodeData(&test1)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	fmt.Printf("%+v\n", es)
}
