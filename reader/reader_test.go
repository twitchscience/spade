package reader

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"
)

const validLine = "Success!"

func TestOversizeToken(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "spade-reader-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()
	gzWriter := gzip.NewWriter(f)
	var longLine string
	for i := 0; i < 12800; i++ {
		longLine += "abcde12345"
	}

	gzWriter.Write([]byte(validLine + "\n"))
	gzWriter.Write([]byte(longLine + "\n"))
	gzWriter.Write([]byte(validLine + "\n"))
	gzWriter.Flush()
	gzWriter.Close()

	r, err := GetFileLogReader(f.Name(), true)
	if err != nil {
		t.Fatal(err)
	}

	p, err := r.ProvideLine()
	if err != nil {
		t.Fatal(err)
	}

	if string(p.Data()) != validLine {
		t.Fatal("Unexpected data:", string(p.Data()), "expected:", validLine)
	}

	p, err = r.ProvideLine()
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Data()) != len(longLine) {
		t.Fatal("Unexpected length:", len(p.Data()), "expected:", len(longLine))
	}

	if string(p.Data()) != longLine {
		t.Fatal("Got correct length but wrong contents of long line")
	}

	p, err = r.ProvideLine()
	if err != nil {
		t.Fatal(err)
	}

	if string(p.Data()) != validLine {
		t.Fatal("Unexpected data:", string(p.Data()))
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
