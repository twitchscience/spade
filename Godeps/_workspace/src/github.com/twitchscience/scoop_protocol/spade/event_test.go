package spade

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"net"
	"testing"
	"time"
)

func randomString(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("while generating random string:", err)
	}
	return base64.URLEncoding.EncodeToString(b)
}

var exEvent = NewEvent(
	time.Unix(1397768380, 0),
	net.ParseIP("222.222.222.222"),
	"192.168.0.1, 222.222.222.222",
	"1",
	randomString(2048),
)

// These exist to ensure that the impact of proxying the chosen
// serialization method is negligible.
//
// Current performance behaviour suggests these pose no significant
// issue:

// $ go test -bench=. github.com/twitchscience/scoop_protocol/spade
//
// Benchmark_Marshal       200000     14846 ns/op
// Benchmark_Unmarshal      50000     47529 ns/op
// Benchmark_MarshalJSON   200000     14743 ns/op
// Benchmark_UnmarshalJSON  50000     47391 ns/op

var byteHolder []byte
var eventHolder Event

func Benchmark_Marshal(b *testing.B) {
	var d []byte
	for i := 0; i < b.N; i++ {
		d, _ = Marshal(exEvent)
	}
	byteHolder = d
}

func Benchmark_Unmarshal(b *testing.B) {
	var e Event
	var d []byte
	d, _ = Marshal(exEvent)
	for i := 0; i < b.N; i++ {
		Unmarshal(d, &e)
	}
	eventHolder = e
}

func Benchmark_MarshalJSON(b *testing.B) {
	var d []byte
	for i := 0; i < b.N; i++ {
		d, _ = json.Marshal(exEvent)
	}
	byteHolder = d
}

func Benchmark_UnmarshalJSON(b *testing.B) {
	var e Event
	var d []byte
	d, _ = Marshal(exEvent)
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &e)
	}
	eventHolder = e
}
