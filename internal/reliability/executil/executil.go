package executil

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type Result struct {
	Command string `json:"command"`
	Stdout  string `json:"stdout,omitempty"`
	Stderr  string `json:"stderr,omitempty"`
}

type Runner interface {
	Run(ctx context.Context, name string, args ...string) (Result, error)
}

type LocalRunner struct{}

func (LocalRunner) Run(ctx context.Context, name string, args ...string) (Result, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	result := Result{Command: ShellCommand(name, args...), Stdout: stdout.String(), Stderr: stderr.String()}
	if err := cmd.Run(); err != nil {
		result.Stdout = stdout.String()
		result.Stderr = stderr.String()
		return result, fmt.Errorf("%s: %w\nstdout:\n%s\nstderr:\n%s", result.Command, err, result.Stdout, result.Stderr)
	}
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	return result, nil
}

func ShellCommand(name string, args ...string) string {
	parts := append([]string{name}, args...)
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = shellQuote(part)
	}
	return strings.Join(quoted, " ")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return !(r == '-' || r == '_' || r == '.' || r == '/' || r == ':' || r == '=' || r == ',' || r == '@' || r == '%' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) == -1 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
