package parser

import (
	"reflect"
	"testing"
)

var logLine string = `10.0.1.69/127.0.0.1 [1381280296.633] "ip=0&data=eyJldmVudCI6ImhlbGxvIn0%3D" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`

func TestVariants(t *testing.T) {
	matches := LexLine([]byte(logLine))
	expected := parseResult{
		Ip:   "10.0.1.69",
		Time: "1381280296",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var1 := `-/10.0.1.69 [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0%3D&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var1))
	expected = parseResult{
		Ip:   "10.0.1.69",
		Time: "1381280296",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var2 := `10.0.1.69/- [1381280296.633] data=eyJldmVudCI6ImhlbGxvIn0=&ip=0 b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var2))
	expected = parseResult{
		Ip:   "10.0.1.69",
		Time: "1381280296",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var3 := `10.0.1.69 [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0=&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var3))
	expected = parseResult{
		Ip:   "10.0.1.69",
		Time: "1381280296",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var4 := `-10.0.1.69| [1381280296.633] "data=eyJldmVudCI6ImhlbGxvIn0=&ip=0" b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var4))
	expected = parseResult{
		Ip:   "10.0.1.69",
		Time: "1381280296",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0="),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}

	var5 := `98.227.19.187 [1397781324.000] data=eyJldmVudCI6ImhlbGxvIn0%3D b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var5))
	expected = parseResult{
		Ip:   "98.227.19.187",
		Time: "1397781324",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
	var6 := `98.227.19.187, 222.222.222.222 [1397781324.000] data=eyJldmVudCI6ImhlbGxvIn0%3D b965f6d8-49e13880-5350734c-fa42601d46b5ddda`
	matches = LexLine([]byte(var6))
	expected = parseResult{
		Ip:   "98.227.19.187",
		Time: "1397781324",
		Data: []byte("eyJldmVudCI6ImhlbGxvIn0%3D"),
		UUID: "b965f6d8-49e13880-5350734c-fa42601d46b5ddda",
	}
	if !reflect.DeepEqual(expected, *matches) {
		t.Errorf("Expecting %v got %v", expected, matches)
	}
}
