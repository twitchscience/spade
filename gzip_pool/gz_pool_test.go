package gzip_pool

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
)

func TestGZPoolSingleThread(t *testing.T) {
	pool := New(1)
	expected := "asdlkfjhasldkjfhalkwejhrflksajdhflkjs"
	var testBuffer bytes.Buffer
	gzipWriter := pool.Get(&testBuffer)
	fmt.Fprint(gzipWriter, expected)

	gzipWriter.Flush()
	gzipWriter.Close()
	pool.Put(gzipWriter)

	r, err := gzip.NewReader(&testBuffer)
	if err != nil {
		t.Fail()
	}
	all, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fail()
	}
	if string(all) != expected {
		t.Errorf("expected %s but got %s\n", all, expected)
	}
}

func BenchmarkGZPoolMultithreaded(b *testing.B) {
	pool := New(2)
	expected := "asdlkfjhasldkjfhalkwejhrflksajdhflkjs"
	var w sync.WaitGroup
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i := 0; i < 8; i++ {
			w.Add(1)
			go func() {
				var testBuffer bytes.Buffer
				gzipWriter := pool.Get(&testBuffer)
				fmt.Fprint(gzipWriter, expected)

				gzipWriter.Flush()
				gzipWriter.Close()
				pool.Put(gzipWriter)

				r, err := gzip.NewReader(&testBuffer)
				if err != nil {
					b.Fail()
				}
				all, err := ioutil.ReadAll(r)
				if err != nil {
					b.Fail()
				}
				if string(all) != expected {
					b.Errorf("expected %s but got %s\n", all, expected)
				}
				w.Done()
			}()
		}
		w.Wait()
	}
	b.ReportAllocs()
}
