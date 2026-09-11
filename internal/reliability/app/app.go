package app

import (
	"fmt"
	"io"
	"strings"
)

const commandName = "mycel-lab"

// Run executes the mycel-lab command and returns a process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		printUsage(stdout)
		return 0
	}

	switch args[0] {
	case "db":
		return runDB(args[1:], stdout, stderr)
	case "import":
		return runImport(args[1:], stdout, stderr)
	case "export":
		return runExport(args[1:], stdout, stderr)
	case "run":
		return runRun(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintln(stdout, "mycel-lab development")
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runDB(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab db <migrate|status> [flags]")
		return 0
	}
	switch args[0] {
	case "migrate", "status":
		fmt.Fprintf(stdout, "db %s: not implemented yet (planned in RH2)\n", args[0])
		return 0
	default:
		fmt.Fprintf(stderr, "unknown db command %q\n", args[0])
		return 2
	}
}

func runImport(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab import [--dry-run] <path>")
		return 0
	}
	path := lastNonFlag(args)
	if path == "" {
		fmt.Fprintln(stderr, "import requires a path")
		return 2
	}
	fmt.Fprintf(stdout, "import %s: not implemented yet (planned in RH3)\n", path)
	return 0
}

func runExport(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab export <cluster-profile|actor-profile|scenario|suite> <name> [--version <version>]")
		return 0
	}
	if len(nonFlagArgs(args)) < 2 {
		fmt.Fprintln(stderr, "export requires a definition kind and name")
		return 2
	}
	positional := nonFlagArgs(args)
	fmt.Fprintf(stdout, "export %s %s: not implemented yet (planned in RH3)\n", positional[0], positional[1])
	return 0
}

func runRun(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab run <scenario|scenario-file|suite> <name-or-path> [flags]")
		return 0
	}
	if len(nonFlagArgs(args)) < 2 {
		fmt.Fprintln(stderr, "run requires a target kind and name/path")
		return 2
	}
	positional := nonFlagArgs(args)
	switch positional[0] {
	case "scenario", "scenario-file", "suite":
		fmt.Fprintf(stdout, "run %s %s: not implemented yet (planned in RH4+)\n", positional[0], positional[1])
		return 0
	default:
		fmt.Fprintf(stderr, "unknown run target %q\n", positional[0])
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `%s designs, runs, observes, and compares long-running MycelDB cluster experiments.

Usage:
  mycel-lab <command> [arguments]

Commands:
  db       Manage the reliability database schema
  import   Import YAML definitions into the reliability catalog
  export   Export catalog definitions as YAML
  run      Run scenarios or suites
  version  Print version information

Use "mycel-lab <command> --help" for command-specific help.
`, commandName)
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "--help" || arg == "-h"
}

func nonFlagArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			if flagTakesValue(arg) && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func flagTakesValue(arg string) bool {
	if strings.Contains(arg, "=") {
		return false
	}
	switch arg {
	case "--database-url", "--artifact-root", "--profile-dir", "--output", "--version":
		return true
	default:
		return false
	}
}

func lastNonFlag(args []string) string {
	positional := nonFlagArgs(args)
	if len(positional) == 0 {
		return ""
	}
	return positional[len(positional)-1]
}
