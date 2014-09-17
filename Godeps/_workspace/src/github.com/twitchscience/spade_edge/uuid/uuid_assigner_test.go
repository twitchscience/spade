package uuid

import (
	"strings"
	"testing"
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
