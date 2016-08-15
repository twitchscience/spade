package main

import (
	"flag"
	"fmt"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/spade/config_fetcher/fetcher"
)

var (
	schemaServerProto    = flag.String("protocol", "http", "The protocol the schema server uses")
	schemaServerHostname = flag.String("hostname", "", "The hostname for the schema server")
	schemaServerPort     = flag.Uint64("port", 80, "The port the schema server is listening on")
	schemaCacheFileName  = flag.String("schemaFile", "table_config.json", "The location of the fetched Schema")
)

func makeBluePrintURL(proto, hostname string, port uint64) (string, error) {
	if hostname == "" {
		return "", fmt.Errorf("No hostname provided")
	}
	return fmt.Sprintf("%s://%s:%d/schemas", proto, hostname, port), nil
}

func main() {
	flag.Parse()
	bpURL, err := makeBluePrintURL(*schemaServerProto, *schemaServerHostname, *schemaServerPort)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create blueprint URL")
	}
	err = fetcher.FetchConfig(fetcher.New(bpURL), *schemaCacheFileName)
	if err != nil {
		logger.WithError(err).Fatal("Failed to fetch config")
	}
}
