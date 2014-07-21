package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/TwitchScience/spade/transformer"
)

var outFile string

func init() {
	flag.StringVar(&outFile, "outFile", "transforms_available.json", "JSON output file of transforms")
}

func main() {
	flag.Parse()
	s, err := json.Marshal(transformer.TransformsAvailable)
	if err != nil {
		log.Fatalf("Got error marshalling: %s", err)
	}
	err = ioutil.WriteFile(outFile, s, 0644)
	if err != nil {
		log.Fatalf("Error writing file: %s", err)
	}
}
