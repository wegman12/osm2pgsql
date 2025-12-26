package main

import (
	"os"

	"github.com/kevinramage/osm2pgsql-go/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
