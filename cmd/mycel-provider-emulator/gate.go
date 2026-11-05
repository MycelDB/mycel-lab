package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/emulator"
	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func runGate(args []string) error {
	flags := flag.NewFlagSet("gate", flag.ContinueOnError)
	name := flags.String("name", "", "synthetic gate name")
	action := flags.String("action", "get", "get or release")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !provider.ValidEmulatorGate(*name) || *action != "get" && *action != "release" || flags.NArg() != 0 {
		return errors.New("invalid gate control")
	}
	method := http.MethodGet
	if *action == "release" {
		method = http.MethodPost
	}
	request, err := http.NewRequest(method, "http://127.0.0.1:18080/control/gates/"+*name, nil)
	if err != nil {
		return errors.New("invalid gate request")
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return errors.New("gate control transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("gate control rejected")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4097))
	decoder.DisallowUnknownFields()
	var state emulator.GateState
	if err := decoder.Decode(&state); err != nil {
		return errors.New("invalid gate response")
	}
	if state.SchemaVersion != 1 || state.Name != *name || state.Sequence < 0 {
		return errors.New("invalid gate identity")
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return errors.New("invalid gate response boundary")
	}
	return json.NewEncoder(os.Stdout).Encode(state)
}
