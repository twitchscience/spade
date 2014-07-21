package main

import (
	"flag"
	"fmt"
	"time"
	"github.com/TwitchScience/spade/table_config"

	"net/http"

	"log"
	"os"

	"github.com/TwitchScience/spade_edge/request_handler"
)

const usage = `A handy tool to verify spade events.`

var (
	help       bool
	configPath string
)

func main() {
	flag.BoolVar(&help, "help", false, "display the help message")
	flag.StringVar(&configPath, "config", "table_config.json", "the location of the table_config.json")
	flag.Parse()

	if help {
		fmt.Println(usage)
		os.Exit(0)
	}

	tables, err := table_config.LoadConfigFromFile(configPath)
	if err != nil {
		log.Fatalf("Failed To Load Config: %v\n", err)
	}
	config, c_err := tables.CompileForParsing()
	if c_err != nil {
		log.Fatalf("Failed To Load Config: %v\n", c_err)
	}

	p := BuildProcessor(config)
	handler := &request_handler.SpadeHandler{
		EdgeLogger: &EZSpadeEdgeLogger{p, &StdoutSpadeWriter{config}},
		Assigner:   request_handler.Assigner,
	}

	// setup server and listen
	server := &http.Server{
		Addr:           ":8888",
		Handler:        handler,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 23, // 8MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
