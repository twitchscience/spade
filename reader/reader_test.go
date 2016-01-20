package reader

import (
	"io/ioutil"
	"testing"
	"compress/gzip"
	"os"
)

const validLine = "Success!"

func TestOversizeToken(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "spade-reader-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func () {
		f.Close()
		os.Remove(f.Name())
	}()
	gzWriter := gzip.NewWriter(f)
	var longLine string
	for i:=0; i < 12800; i++ {
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

	// Read a good line, then skip a bad line, then a good line
	p, err := r.ProvideLine()
	if err != nil {
		t.Fatal(err)
	}

	if string(p.Data()) != validLine {
		t.Fatal("Unexpected data:", string(p.Data()))
	}

	_, err = r.ProvideLine()
	if (err != SkipLongLine{}) {
		t.Fatal(err)
	}

	p, err = r.ProvideLine()
	if err != nil {
		t.Fatal(err)
	}

	if string(p.Data()) != validLine {
		t.Fatal("Unexpected data:", string(p.Data()))
	}
}
