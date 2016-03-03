package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/twitchscience/spade/table_config"

	"net/http"

	"log"
	"os"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/spade_edge/request_handler"
)

const usage = `A handy tool to verify spade events.`

var (
	help       bool
	configPath string
)

type HeaderInjectorHandler struct {
	HeadersToInject map[string]string
	Handler         http.Handler
}

func (h *HeaderInjectorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for k, v := range h.HeadersToInject {
		r.Header.Add(k, v)
	}
	r.Header.Add("X-ORIGINAL-MSEC", fmt.Sprintf("%d.000", time.Now().Unix()))
	h.Handler.ServeHTTP(w, r)
}

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

	config, versions, c_err := tables.CompileForParsing()
	if c_err != nil {
		log.Fatalf("Failed To Load Config: %v\n", c_err)
	}

	p := BuildProcessor(config, versions)
	handler := &request_handler.SpadeHandler{
		EdgeLogger: &EZSpadeEdgeLogger{p, &StdoutSpadeWriter{config}},
		Assigner:   request_handler.Assigner,
		StatLogger: &statsd.NoopClient{},
	}

	headerHandler := &HeaderInjectorHandler{
		HeadersToInject: map[string]string{
			"X-Forwarded-For": "22.22.22.22",
		},
		Handler: handler,
	}

	// setup server and listen
	server := &http.Server{
		Addr:           ":8888",
		Handler:        headerHandler,
		ReadTimeout:    1 * time.Second,
		WriteTimeout:   1 * time.Second,
		MaxHeaderBytes: 1 << 23, // 8MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
