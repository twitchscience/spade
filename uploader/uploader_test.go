package uploader

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"
)

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
		actual, err := extractEventVersion(input)
		if err != nil {
			t.Errorf("didn't expect an error, got: %v", err)
		}
		if actual != expected {
			t.Errorf("expected %d but got %d", expected, actual)
		}
	}

	testFunc(5, "/opt/science/spade/data/events/minute-watched.v5.gz")
	testFunc(22, "/opt/science/spade/data/upload/minute-watched.v22.gz")
	testFunc(16, "/opt/science/spade/data/upload/minute-watched.v16.gz.gz")
	testFunc(199, "minute-watched.v199.gz")
	testFunc(0, "/opt/science/spade/data/events/minute-watched.v0")
}

func copy(from, to string) error {
	data, err := ioutil.ReadFile(from)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(to, data, 0666)
}

func readgz(path string, t *testing.T) string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Error opening file to read: %s", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("Error creating gz reader: %s", err)
	}
	defer func() { _ = gz.Close() }()
	b, err := ioutil.ReadAll(gz)
	if err != nil {
		t.Fatalf("Error reading gzfile: %s", err)
	}
	return string(b)
}

func TestSalvageData(t *testing.T) {
	// test_data/minute-watched.v0.gz retrieved by kill-9ing an integration
	// processor. test_data/minute-watched.v0.recovered.gz is the recovered data
	// with the last line truncated, then gzipped, the expected result of
	// salvageData
	corruptedPath := "testdata/minute-watched.v0.corrupted.gz"
	path := "testdata/minute-watched.v0.gz"
	expectedPath := "testdata/minute-watched.v0.recovered.gz"
	err := copy(corruptedPath, path)
	defer func() { _ = os.Remove(path) }()
	if err != nil {
		t.Fatalf("error copying %s to %s", corruptedPath, path)
	}
	reportedSalvaged, err := salvageData(path)
	if !reportedSalvaged || err != nil {
		t.Fatalf("nothing salvaged or error salvaging data: (%t, %s)", reportedSalvaged, err)
	}
	salvaged := readgz(path, t)
	expected := readgz(expectedPath, t)
	if salvaged != expected {
		t.Fatalf("expected salvaged data to be the same as expected, but they differ: %s \n\nvs\n\n%s", salvaged, expected)
	}
}

func TestSalvageEmpty(t *testing.T) {
	// Create a not-gz file and try to salvage it.
	path := "testdata/testSalvageData.csv.gz"
	f, err := os.Create(path)
	defer func() { _ = os.Remove(path) }()
	if err != nil {
		t.Errorf("problem creating file %s", err)
	}
	_, _ = f.Write([]byte("texterino"))
	_ = f.Close()

	_, err = salvageData(path)
	if err != nil {
		t.Error("Expected error from salvage data, but received nil")
	}
}
