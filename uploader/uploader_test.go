package uploader

import "testing"

func TestExtractEventname(t *testing.T) {
	testFunc := func(expected, input string) {
		if extractEventName(input) != expected {
			t.Errorf("expected %s but got %s", expected, extractEventName(input))
		}
	}

	testFunc("minute-watched", "/opt/science/spade/data/events/minute-watched.v5.gz")
	testFunc("minute-watched", "/opt/science/spade/data/upload/minute-watched.v22.gz")
	testFunc("minute-watched", "/opt/science/spade/data/upload/minute-watched.v16.gz.gz")
	testFunc("minute-watched", "minute-watched.v199.gz")
	testFunc("minute-watched", "/opt/science/spade/data/events/minute-watched.v0")

}

func TestExtractEventVersion(t *testing.T) {
	testFunc := func(expected int, input string) {
		if extractEventVersion(input) != expected {
			t.Errorf("expected %d but got %d", expected, extractEventVersion(input))
		}
	}

	testFunc(5, "/opt/science/spade/data/events/minute-watched.v5.gz")
	testFunc(22, "/opt/science/spade/data/upload/minute-watched.v22.gz")
	testFunc(16, "/opt/science/spade/data/upload/minute-watched.v16.gz.gz")
	testFunc(199, "minute-watched.v199.gz")
	testFunc(0, "/opt/science/spade/data/events/minute-watched.v0")
}
