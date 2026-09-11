package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/catalog"
	"github.com/MycelDB/mycel-lab/internal/reliability/metrics"
	"github.com/MycelDB/mycel-lab/internal/reliability/runner"
	"github.com/MycelDB/mycel-lab/internal/reliability/store"
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
	case "list":
		return runList(args[1:], stdout, stderr)
	case "show":
		return runShow(args[1:], stdout, stderr)
	case "delete":
		return runDelete(args[1:], stdout, stderr)
	case "run":
		return runRun(args[1:], stdout, stderr)
	case "runs":
		return runRuns(args[1:], stdout, stderr)
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
		// Valid subcommands; open the database below.
	default:
		fmt.Fprintf(stderr, "unknown db command %q\n", args[0])
		return 2
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	switch args[0] {
	case "migrate":
		if err := st.Migrate(ctx); err != nil {
			fmt.Fprintf(stderr, "db migrate: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "database migrations applied")
		return 0
	case "status":
		statuses, err := st.MigrationStatus(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "db status: %v\n", err)
			return 1
		}
		for _, status := range statuses {
			state := "pending"
			if status.Applied {
				state = "applied"
			}
			fmt.Fprintf(stdout, "%s\t%s\t%s\n", status.Version, state, status.Name)
		}
		return 0
	default:
		panic("validated db command reached impossible default")
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
	ctx := context.Background()
	var st store.Store
	if !hasFlag(args, "--dry-run") {
		pg, ok := openPostgresFromArgs(ctx, args, stderr)
		if !ok {
			return 2
		}
		defer pg.Close()
		st = pg
	}
	results, err := catalog.ImportPath(ctx, st, path, catalog.ImportOptions{DryRun: hasFlag(args, "--dry-run"), ProfileDirs: flagValues(args, "--profile-dir")})
	if err != nil {
		fmt.Fprintf(stderr, "import: %v\n", err)
		return 1
	}
	failed := false
	for _, result := range results {
		if result.Error != nil {
			failed = true
			fmt.Fprintf(stderr, "%s\tERROR\t%v\n", result.Path, result.Error)
			continue
		}
		version := ""
		if result.Version > 0 {
			version = fmt.Sprintf("\tv%d", result.Version)
		}
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s%s\n", result.Action, result.Kind, result.Name, result.Hash, version)
	}
	if failed {
		return 1
	}
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
	kind, err := parseDefinitionKind(positional[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	version, err := parseVersion(flagValue(args, "--version"))
	if err != nil {
		fmt.Fprintf(stderr, "invalid --version: %v\n", err)
		return 2
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	data, err := catalog.ExportDefinition(ctx, st, kind, positional[1], version)
	if err != nil {
		fmt.Fprintf(stderr, "export: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, string(data))
	return 0
}

func runList(args []string, stdout, stderr io.Writer) int {
	positional := nonFlagArgs(args)
	if len(positional) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab list <cluster-profile|actor-profile|scenario|suite> [--database-url <url>]")
		return 0
	}
	kind, err := parseDefinitionKind(positional[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	defs, err := st.ListDefinitions(ctx, kind)
	if err != nil {
		fmt.Fprintf(stderr, "list: %v\n", err)
		return 1
	}
	for _, def := range defs {
		fmt.Fprintf(stdout, "%s\tv%d\tused=%d\tcan_edit=%t\t%s\n", def.Name, def.Version, def.UsedByRunCount, def.CanEdit(), def.SpecHash)
	}
	return 0
}

func runShow(args []string, stdout, stderr io.Writer) int {
	if len(nonFlagArgs(args)) < 2 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab show <cluster-profile|actor-profile|scenario|suite> <name> [--version <version>|latest]")
		return 0
	}
	positional := nonFlagArgs(args)
	kind, err := parseDefinitionKind(positional[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	version, err := parseVersion(flagValue(args, "--version"))
	if err != nil {
		fmt.Fprintf(stderr, "invalid --version: %v\n", err)
		return 2
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	data, err := catalog.ExportDefinition(ctx, st, kind, positional[1], version)
	if err != nil {
		fmt.Fprintf(stderr, "show: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, string(data))
	return 0
}

func runDelete(args []string, stdout, stderr io.Writer) int {
	if len(nonFlagArgs(args)) < 2 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab delete <cluster-profile|actor-profile|scenario|suite> <name> [--version <version>]")
		return 0
	}
	positional := nonFlagArgs(args)
	kind, err := parseDefinitionKind(positional[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	version, err := parseVersion(flagValue(args, "--version"))
	if err != nil {
		fmt.Fprintf(stderr, "invalid --version: %v\n", err)
		return 2
	}
	if version == 0 {
		fmt.Fprintln(stderr, "delete requires an explicit --version")
		return 2
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	if err := st.DeleteDefinition(ctx, kind, positional[1], version); err != nil {
		fmt.Fprintf(stderr, "delete: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "deleted %s %s v%d\n", kind, positional[1], version)
	return 0
}

func runRun(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab run <scenario|scenario-file|suite|suite-file> <name-or-path> [flags]")
		return 0
	}
	if len(nonFlagArgs(args)) < 2 {
		fmt.Fprintln(stderr, "run requires a target kind and name/path")
		return 2
	}
	positional := nonFlagArgs(args)
	switch positional[0] {
	case "scenario-file":
		resolved, err := catalog.ResolveScenarioFile(positional[1], catalog.ResolveOptions{ProfileDirs: flagValues(args, "--profile-dir")})
		if err != nil {
			fmt.Fprintf(stderr, "resolve scenario-file: %v\n", err)
			return 1
		}
		result, err := runner.RunScenario(context.Background(), resolved, runner.Options{DryRun: hasFlag(args, "--dry-run"), ArtifactRoot: flagValue(args, "--artifact-root"), ConfirmDestructive: hasFlag(args, "--confirm-destructive"), KeepEnvironmentOnFailure: hasFlag(args, "--keep-environment-on-failure")})
		if err != nil {
			fmt.Fprintf(stderr, "run scenario-file: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "run %s %s: %s\nartifacts: %s\n", positional[0], positional[1], result.Status, result.ArtifactRoot)
		return 0
	case "suite-file":
		result, err := runner.RunSuiteFile(context.Background(), positional[1], runner.Options{DryRun: hasFlag(args, "--dry-run"), ArtifactRoot: flagValue(args, "--artifact-root"), ConfirmDestructive: hasFlag(args, "--confirm-destructive"), KeepEnvironmentOnFailure: hasFlag(args, "--keep-environment-on-failure")})
		if err != nil {
			fmt.Fprintf(stderr, "run suite-file: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "suite %s: %d scenario(s)\n", result.SuiteName, len(result.Results))
		for _, scenario := range result.Results {
			fmt.Fprintf(stdout, "- %s: %s artifacts=%s\n", scenario.ScenarioName, scenario.Status, scenario.ArtifactRoot)
		}
		return 0
	case "suite":
		suitePath := positional[1]
		if !looksLikePath(suitePath) {
			suitePath = filepath.Join("tests", "reliability", "suites", suitePath+".yaml")
		}
		result, err := runner.RunSuiteFile(context.Background(), suitePath, runner.Options{DryRun: hasFlag(args, "--dry-run"), ArtifactRoot: flagValue(args, "--artifact-root"), ConfirmDestructive: hasFlag(args, "--confirm-destructive"), KeepEnvironmentOnFailure: hasFlag(args, "--keep-environment-on-failure")})
		if err != nil {
			fmt.Fprintf(stderr, "run suite: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "suite %s: %d scenario(s)\n", result.SuiteName, len(result.Results))
		for _, scenario := range result.Results {
			fmt.Fprintf(stdout, "- %s: %s artifacts=%s\n", scenario.ScenarioName, scenario.Status, scenario.ArtifactRoot)
		}
		return 0
	case "scenario":
		scenarioPath := positional[1]
		if !looksLikePath(scenarioPath) {
			scenarioPath = filepath.Join("tests", "reliability", "scenarios", scenarioPath+".yaml")
		}
		resolved, err := catalog.ResolveScenarioFile(scenarioPath, catalog.ResolveOptions{ProfileDirs: flagValues(args, "--profile-dir")})
		if err != nil {
			fmt.Fprintf(stderr, "resolve scenario: %v\n", err)
			return 1
		}
		result, err := runner.RunScenario(context.Background(), resolved, runner.Options{DryRun: hasFlag(args, "--dry-run"), ArtifactRoot: flagValue(args, "--artifact-root"), ConfirmDestructive: hasFlag(args, "--confirm-destructive"), KeepEnvironmentOnFailure: hasFlag(args, "--keep-environment-on-failure")})
		if err != nil {
			fmt.Fprintf(stderr, "run scenario: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "run scenario %s: %s\nartifacts: %s\n", positional[1], result.Status, result.ArtifactRoot)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown run target %q\n", positional[0])
		return 2
	}
}

func runRuns(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprintln(stdout, "Usage: mycel-lab runs <list|show|compare> [arguments]")
		return 0
	}
	ctx := context.Background()
	st, ok := openPostgresFromArgs(ctx, args, stderr)
	if !ok {
		return 2
	}
	defer st.Close()
	switch args[0] {
	case "list":
		runs, err := st.ListRuns(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "runs list: %v\n", err)
			return 1
		}
		for _, run := range runs {
			finished := ""
			if run.FinishedAt != nil {
				finished = run.FinishedAt.Format("2006-01-02T15:04:05Z07:00")
			}
			fmt.Fprintf(stdout, "%s\t%s\tv%d\t%s\tstarted=%s\tfinished=%s\n", run.ID, run.ScenarioName, run.ScenarioVersion, run.Status, run.StartedAt.Format("2006-01-02T15:04:05Z07:00"), finished)
		}
		return 0
	case "show":
		positional := nonFlagArgs(args)
		if len(positional) < 2 {
			fmt.Fprintln(stderr, "runs show requires a run id")
			return 2
		}
		details, err := st.GetRun(ctx, positional[1])
		if err != nil {
			fmt.Fprintf(stderr, "runs show: %v\n", err)
			return 1
		}
		summary := metrics.FromRunDetails(details)
		fmt.Fprintf(stdout, "run: %s\nscenario: %s v%d\nstatus: %s\nstarted: %s\n", details.Run.ID, details.Run.ScenarioName, details.Run.ScenarioVersion, details.Run.Status, details.Run.StartedAt.Format("2006-01-02T15:04:05Z07:00"))
		if details.Run.FinishedAt != nil {
			fmt.Fprintf(stdout, "finished: %s\n", details.Run.FinishedAt.Format("2006-01-02T15:04:05Z07:00"))
		}
		fmt.Fprintf(stdout, "events: %d\nmetrics: %d\nartifacts: %d\ncommits/sec: %.3f\ntransient errors: %d\npermanent errors: %d\n", len(details.Events), len(details.Metrics), len(details.Artifacts), summary.CommitsPerSecond, summary.TransientErrors, summary.PermanentErrors)
		for _, artifact := range details.Artifacts {
			fmt.Fprintf(stdout, "artifact: %s\t%s\n", artifact.Type, artifact.Path)
		}
		return 0
	case "compare":
		positional := nonFlagArgs(args)
		if len(positional) < 3 {
			fmt.Fprintln(stderr, "runs compare requires two run ids")
			return 2
		}
		left, err := st.GetRun(ctx, positional[1])
		if err != nil {
			fmt.Fprintf(stderr, "runs compare: %v\n", err)
			return 1
		}
		right, err := st.GetRun(ctx, positional[2])
		if err != nil {
			fmt.Fprintf(stderr, "runs compare: %v\n", err)
			return 1
		}
		cmp := metrics.Compare(metrics.FromRunDetails(left), metrics.FromRunDetails(right))
		fmt.Fprintf(stdout, "compare: %s -> %s\ncommits/sec delta: %.3f\ntransient error delta: %d\npermanent error delta: %d\n", positional[1], positional[2], cmp.Delta.CommitsPerSecond, cmp.Delta.TransientErrors, cmp.Delta.PermanentErrors)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown runs command %q\n", args[0])
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
  list     List catalog definitions
  show     Show one catalog definition as YAML
  delete   Delete one unused catalog definition version
  run      Run scenarios or suites
  runs     List, show, and compare persisted runs
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

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

func looksLikePath(value string) bool {
	return strings.HasSuffix(value, ".yaml") || strings.HasSuffix(value, ".yml") || strings.Contains(value, string(os.PathSeparator))
}

func openPostgresFromArgs(ctx context.Context, args []string, stderr io.Writer) (*store.PostgresStore, bool) {
	databaseURL := flagValue(args, "--database-url")
	if databaseURL == "" {
		databaseURL = os.Getenv("MYCEL_LAB_DATABASE_URL")
	}
	if databaseURL == "" {
		databaseURL = os.Getenv("MYCEL_RELIABILITY_DATABASE_URL")
	}
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		fmt.Fprintln(stderr, "database URL is required via --database-url, MYCEL_LAB_DATABASE_URL, MYCEL_RELIABILITY_DATABASE_URL, or DATABASE_URL")
		return nil, false
	}
	st, err := store.OpenPostgres(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "open database: %v\n", err)
		return nil, false
	}
	return st, true
}

func parseDefinitionKind(value string) (store.DefinitionKind, error) {
	switch value {
	case "cluster-profile", "cluster", "clusters":
		return store.KindClusterProfile, nil
	case "actor-profile", "actor", "actors":
		return store.KindActorProfile, nil
	case "scenario", "scenarios":
		return store.KindScenario, nil
	case "suite", "suites":
		return store.KindSuite, nil
	default:
		return "", fmt.Errorf("unsupported definition kind %q", value)
	}
}

func parseVersion(value string) (int, error) {
	if value == "" || value == "latest" {
		return 0, nil
	}
	version, err := strconv.Atoi(value)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("must be a positive integer or latest")
	}
	return version, nil
}

func flagValue(args []string, name string) string {
	values := flagValues(args, name)
	if len(values) == 0 {
		return ""
	}
	return values[len(values)-1]
}

func flagValues(args []string, name string) []string {
	var values []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == name && i+1 < len(args) {
			values = append(values, args[i+1])
			i++
			continue
		}
		if strings.HasPrefix(arg, name+"=") {
			values = append(values, strings.TrimPrefix(arg, name+"="))
		}
	}
	return values
}

func lastNonFlag(args []string) string {
	positional := nonFlagArgs(args)
	if len(positional) == 0 {
		return ""
	}
	return positional[len(positional)-1]
}
