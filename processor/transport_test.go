package processor

import (
	"crypto/rand"
	"encoding/json"
	r "math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/twitchscience/spade/parser"
	"github.com/twitchscience/spade/reporter"
)

func TestGobTransport(t *testing.T) {
	g := NewGobTransport(NewBufferedTransport())
	testMsg := &parser.MixpanelEvent{
		Pstart:     time.Now(),
		EventTime:  json.Number(123),
		UUID:       "uuid",
		ClientIp:   "222.222.222.222",
		Event:      "test",
		Properties: json.RawMessage([]byte("Test")),
		Failure:    reporter.NONE,
	}
	var receive parser.MixpanelEvent
	err := g.Write(testMsg)
	if err != nil {
		t.Fatal(err)
	}
	err = g.Read(&receive)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(testMsg, &receive) {
		t.Fatalf("expected %+v but got %+v\n", testMsg, &receive)
	}
}

func TestDataLeakTransport(t *testing.T) {
	wg := sync.WaitGroup{}
	g := NewGobTransport(NewBufferedTransport())
	randomEvents := func() *parser.MixpanelEvent {
		b := make([]byte, r.Intn(1024))
		rand.Read(b)
		return &parser.MixpanelEvent{
			Pstart:     time.Now(),
			EventTime:  json.Number(123),
			UUID:       "uuid",
			ClientIp:   "222.222.222.222",
			Event:      "test",
			Properties: json.RawMessage(b),
			Failure:    reporter.FailMode(r.Intn(9)),
		}
	}
	events := make([]parser.MixpanelEvent, 1000)
	for i, _ := range events {
		events[i] = *randomEvents()
	}
	// reader
	wg.Add(len(events))
	ticker := time.Tick(10 * time.Microsecond)
	go func() {
		for _, spot := range events {
			func() {
				var event parser.MixpanelEvent
				for {
					select {
					case <-ticker:
						err := g.Read(&event)
						if err == nil {
							if !reflect.DeepEqual(&spot, &event) {
								t.Fatalf("expected %+v but got %+v\n", &spot, &event)
							}
							wg.Done()
							return
						}
					}
				}
			}()
		}
	}()
	// writer
	go func() {
		for _, spot := range events {
			err := g.Write(&spot)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(1 * time.Microsecond)
		}
	}()
	wg.Wait()
}
