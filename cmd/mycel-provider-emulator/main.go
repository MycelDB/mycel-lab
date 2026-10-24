package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
)

const maxScriptBytes = 1 << 20

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "dump":
		err = runDump(os.Args[2:])
	case "health":
		err = runHealth(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := flags.String("listen", envValue("MYCEL_EMULATOR_LISTEN", ":18080"), "listen address")
	dimensionsDefault, _ := strconv.Atoi(envValue("MYCEL_EMULATOR_DIMENSIONS", "8"))
	dimensions := flags.Int("dimensions", dimensionsDefault, "embedding dimensions")
	scriptFile := flags.String("script", envValue("MYCEL_EMULATOR_SCRIPT_FILE", ""), "script JSON path")
	observations := flags.String("observations", envValue("MYCEL_EMULATOR_OBSERVATIONS", "/var/lib/mycel-emulator/observations.jsonl"), "sanitized observation JSONL path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	raw, err := loadScript(*scriptFile, os.Getenv("MYCEL_EMULATOR_SCRIPT_B64"))
	if err != nil {
		return err
	}
	script, err := emulator.LoadProtocolScript(raw)
	if err != nil {
		return fmt.Errorf("load emulator script: %w", err)
	}
	handler, err := emulator.NewOpenAIHandler(emulator.OpenAIConfig{Dimensions: *dimensions, Script: script, ObservationsPath: *observations})
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	fmt.Fprintf(os.Stdout, "provider emulator listening=%s dimensions=%d script_sha256=%s\n", *listen, *dimensions, hex.EncodeToString(hash[:]))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return emulator.Serve(ctx, *listen, handler)
}

func runDump(args []string) error {
	flags := flag.NewFlagSet("dump", flag.ContinueOnError)
	path := flags.String("file", envValue("MYCEL_EMULATOR_OBSERVATIONS", "/var/lib/mycel-emulator/observations.jsonl"), "observation JSONL path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	file, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(os.Stdout, io.LimitReader(file, 64<<20))
	return err
}

func runHealth(args []string) error {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:18080/healthz", "health URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(*url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health status %d", response.StatusCode)
	}
	return nil
}

func loadScript(path, encoded string) ([]byte, error) {
	if strings.TrimSpace(encoded) != "" {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, fmt.Errorf("decode MYCEL_EMULATOR_SCRIPT_B64: %w", err)
		}
		if len(raw) > maxScriptBytes {
			return nil, fmt.Errorf("emulator script exceeds %d bytes", maxScriptBytes)
		}
		return raw, nil
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("emulator script file or MYCEL_EMULATOR_SCRIPT_B64 is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxScriptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxScriptBytes {
		return nil, fmt.Errorf("emulator script exceeds %d bytes", maxScriptBytes)
	}
	return raw, nil
}

func envValue(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: mycel-provider-emulator <serve|dump|health> [flags]")
}
