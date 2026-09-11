package main

import (
	"os"

	"github.com/MycelDB/mycel-lab/internal/reliability/app"
)

func main() {
	os.Exit(app.Run(os.Args[1:], os.Stdout, os.Stderr))
}
