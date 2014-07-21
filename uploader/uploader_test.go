package uploader

import "testing"

func TestExtractEventname(t *testing.T) {
	testFunc := func(expected, input string) {
		if extractEventName(input) != expected {
			t.Errorf("expected %s but got %s", expected, extractEventName(input))
		}
	}

	testFunc("minute-watched", "/opt/science/spade/data/events/minute-watched.gz")
	testFunc("minute-watched", "/opt/science/spade/data/upload/minute-watched.gz")
	testFunc("minute-watched", "/opt/science/spade/data/upload/minute-watched.gz.gz")

	testFunc("minute-watched", "minute-watched.gz")
	testFunc("minute-watched", "/opt/science/spade/data/events/minute-watched")

}
