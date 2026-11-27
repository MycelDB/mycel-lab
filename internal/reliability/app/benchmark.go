package app

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/MycelDB/mycel-lab/internal/reliability/semanticbench"
)

func runBenchmark(args []string, stdout, stderr io.Writer) int {
	const usage = "Usage: mycel-lab benchmark compare <report.json> <report.json> [report.json ...]"
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, usage)
		return 0
	}
	if len(args) < 3 || args[0] != "compare" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	reports := make([]semanticbench.Report, 0, len(args)-1)
	for _, path := range args[1:] {
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		var report semanticbench.Report
		if err := json.Unmarshal(raw, &report); err != nil {
			fmt.Fprintf(stderr, "decode %s: %v\n", path, err)
			return 1
		}
		reports = append(reports, report)
	}
	markdown, err := semanticbench.CompareMarkdown(reports)
	if err == nil {
		_, err = io.WriteString(stdout, markdown)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
