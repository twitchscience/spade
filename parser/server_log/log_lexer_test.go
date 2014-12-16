package server_log

import (
	"reflect"
	"testing"
)

var logLine string = `10.0.1.69/127.0.0.1 [1381280296.633] "ip=0&data=eyJldmVudCI6ImhlbGxvIn0%3D" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`

func TestVariants(t *testing.T) {
	matches := LexLine([]byte(logLine))
	expected := parseResult{
		ip:   "10.0.1.69",
		when: "1381280296",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var1 := `-/10.0.1.69 [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0%3D&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var1))
	expected = parseResult{
		ip:   "10.0.1.69",
		when: "1381280296",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var2 := `10.0.1.69/- [1381280296.633] data=eyJldmVudCI6ImhlbGxvIn0=&ip=0 b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var2))
	expected = parseResult{
		ip:   "10.0.1.69",
		when: "1381280296",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var3 := `10.0.1.69 [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0=&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var3))
	expected = parseResult{
		ip:   "10.0.1.69",
		when: "1381280296",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var4 := `-10.0.1.69| [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0=&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var4))
	expected = parseResult{
		ip:   "10.0.1.69",
		when: "1381280296",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}

	var5 := `98.227.19.187 [1397781324.000] data=eyJldmVudCI6ImhlbGxvIn0%3D b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var5))
	expected = parseResult{
		ip:   "98.227.19.187",
		when: "1397781324",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var6 := `98.227.19.187, 222.222.222.222 [1397781324.000] data=eyJldmVudCI6ImhlbGxvIn0%3D b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var6))
	expected = parseResult{
		ip:   "98.227.19.187",
		when: "1397781324",
		data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var7 := `98.227.19.187, 222.222.222.222 [1397781324.000] data=ip=1&data=eyJldmVudCI6ImhlbGxvIn0%3D b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var7))
	expected = parseResult{
		ip:   "98.227.19.187",
		when: "1397781324",
		data: []byte("ip=1"),
		uuid: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %+v got %+v", expected, matches)
	}
}
