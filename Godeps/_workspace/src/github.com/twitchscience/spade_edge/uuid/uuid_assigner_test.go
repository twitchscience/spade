package uuid

import (
	"strings"
	"testing"
	"time"
)

func TestUUIDAssigner(t *testing.T) {
	a := StartUUIDAssigner("test", "")
	parts := strings.Split(a.Assign(), "-")
	if len(parts[2]) < 8 {
		t.Logf("time was only %d", len(parts[1]))
		t.Fail()
	}

}

func TestUUIDAssignerLongRun(t *testing.T) {
	a := StartUUIDAssigner("stesetsetsetasldjkhafskdjhf", "Sedlhjsdfkj")
	parts := strings.Split(a.Assign(), "-")
	if len(parts[0]) > 8 {
		t.Logf("host was > %d", len(parts[0]))
		t.Fail()
	}
	if len(parts[1]) > 8 {
		t.Logf("cluster was > %d", len(parts[1]))
		t.Fail()
	}
	if len(parts[2]) < 8 {
		t.Logf("time was only %d", len(parts[1]))
		t.Fail()
	}
}

func TestUUIDTick(t *testing.T) {
	// The time portion of the UUID should start at 0 and reset to 1 every second-ish.
	a := StartUUIDAssigner("test-host", "test-cluster")
	if e := strings.Split(a.Assign(), "-")[3]; e != "0" {
		t.Fatalf("time did not start at 0: %s", e)
	}
	if e := strings.Split(a.Assign(), "-")[3]; e != "1" {
		t.Fatalf("time did not increment: %s", e)
	}
	time.Sleep(1 * time.Second)
	if e := strings.Split(a.Assign(), "-")[3]; e != "2" {
		t.Fatalf("expected one more uuid for this tick: %s", e)
	}
	if e := strings.Split(a.Assign(), "-")[3]; e != "1" {
		t.Fatalf("time did not reset to 0: %s", e)
	}
}
