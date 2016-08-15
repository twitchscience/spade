package main

import "testing"

func TestBluePrintURL(t *testing.T) {
	// error if hostname is nil
	if _, err := makeBluePrintURL("http", "", 8080); err == nil {
		t.Error("Expected empty hostname to cause an error")
	}

	url, err := makeBluePrintURL("http", "foo", 8080)
	if err != nil {
		t.Errorf("Unexpected error while creating URL: %s", err)
	}

	expected := "http://foo:8080/schemas"
	if url != expected {
		t.Errorf("Expected %s, got %s", expected, url)
	}
}
