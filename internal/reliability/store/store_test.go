package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/MycelDB/mycel-lab/internal/reliability/migrations"
)

func TestMemoryDefinitionLifecycle(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	first := Definition{Kind: KindScenario, Name: "example", SpecHash: "h1", SpecJSON: []byte(`{"name":"example"}`)}
	created, err := st.UpsertDefinition(ctx, first)
	if err != nil {
		t.Fatalf("UpsertDefinition(create) error = %v", err)
	}
	if created.Status != UpsertCreated || created.Definition.Version != 1 {
		t.Fatalf("create = %+v, want created v1", created)
	}
	noop, err := st.UpsertDefinition(ctx, first)
	if err != nil {
		t.Fatalf("UpsertDefinition(noop) error = %v", err)
	}
	if noop.Status != UpsertNoop || noop.Definition.Version != 1 {
		t.Fatalf("noop = %+v, want noop v1", noop)
	}
	updated, err := st.UpsertDefinition(ctx, Definition{Kind: KindScenario, Name: "example", SpecHash: "h2", SpecJSON: []byte(`{"name":"example","changed":true}`)})
	if err != nil {
		t.Fatalf("UpsertDefinition(update) error = %v", err)
	}
	if updated.Status != UpsertUpdated || updated.Definition.Version != 1 {
		t.Fatalf("update = %+v, want updated v1", updated)
	}
	if err := st.MarkDefinitionUsed(ctx, KindScenario, "example", 1); err != nil {
		t.Fatalf("MarkDefinitionUsed() error = %v", err)
	}
	if err := st.DeleteDefinition(ctx, KindScenario, "example", 1); !errors.Is(err, ErrDefinitionInUse) {
		t.Fatalf("DeleteDefinition(used) error = %v, want ErrDefinitionInUse", err)
	}
	second, err := st.UpsertDefinition(ctx, Definition{Kind: KindScenario, Name: "example", SpecHash: "h3", SpecJSON: []byte(`{"name":"example","changed":"again"}`)})
	if err != nil {
		t.Fatalf("UpsertDefinition(second version) error = %v", err)
	}
	if second.Status != UpsertCreated || second.Definition.Version != 2 {
		t.Fatalf("second = %+v, want created v2", second)
	}
}

func TestMemoryRunDetailsLifecycle(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	run := Run{ID: "run-1", ScenarioName: "example", ScenarioVersion: 1, Status: "running", Seed: 1, ResolvedJSON: []byte(`{"kind":"ResolvedScenario"}`)}
	if err := st.CreateRun(ctx, run, nil, nil); err != nil {
		t.Fatalf("CreateRun() error=%v", err)
	}
	if err := st.AppendEvent(ctx, RunEvent{RunID: run.ID, EventType: "phase-started", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("AppendEvent() error=%v", err)
	}
	if err := st.AppendMetric(ctx, RunMetric{RunID: run.ID, Name: "commits", Value: 3, Labels: []byte(`{}`)}); err != nil {
		t.Fatalf("AppendMetric() error=%v", err)
	}
	if err := st.AppendArtifact(ctx, RunArtifact{RunID: run.ID, Type: "file", Path: "result.json", Metadata: []byte(`{}`)}); err != nil {
		t.Fatalf("AppendArtifact() error=%v", err)
	}
	if err := st.FinishRun(ctx, run.ID, "passed"); err != nil {
		t.Fatalf("FinishRun() error=%v", err)
	}
	runs, err := st.ListRuns(ctx)
	if err != nil {
		t.Fatalf("ListRuns() error=%v", err)
	}
	if len(runs) != 1 || runs[0].Status != "passed" || runs[0].FinishedAt == nil {
		t.Fatalf("runs=%+v, want one finished passed run", runs)
	}
	details, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun() error=%v", err)
	}
	if len(details.Events) != 1 || len(details.Metrics) != 1 || len(details.Artifacts) != 1 {
		t.Fatalf("details=%+v", details)
	}
}

func TestIntegrationPostgresMigrateDefinitionsAndRunRows(t *testing.T) {
	databaseURL := os.Getenv("MYCEL_RELIABILITY_TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("MYCEL_LAB_TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("set MYCEL_RELIABILITY_TEST_DATABASE_URL or MYCEL_LAB_TEST_DATABASE_URL to run Postgres integration tests")
	}
	ctx := context.Background()
	st, err := OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	defer st.Close()
	if err := migrations.Apply(ctx, st.DB()); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	def, err := st.UpsertDefinition(ctx, Definition{Kind: KindScenario, Name: "integration-example", SpecHash: "h1", SpecJSON: []byte(`{"kind":"Scenario"}`), ResolvedJSON: []byte(`{"kind":"ResolvedScenario"}`)})
	if err != nil {
		t.Fatalf("UpsertDefinition() error = %v", err)
	}
	if def.Definition.Version == 0 {
		t.Fatalf("definition version was not set: %+v", def)
	}
	listed, err := st.ListDefinitions(ctx, KindScenario)
	if err != nil {
		t.Fatalf("ListDefinitions() error = %v", err)
	}
	if len(listed) == 0 {
		t.Fatal("ListDefinitions() returned no rows")
	}
	runID := "integration-run-store-test"
	_ = deleteRunForTest(ctx, st, runID)
	if err := st.CreateRun(ctx, Run{ID: runID, ScenarioName: "integration-example", ScenarioVersion: def.Definition.Version, Status: "running", Seed: 1, ResolvedJSON: []byte(`{"kind":"ResolvedScenario"}`)}, []RunActorProfile{{ActorGroupName: "writers", ActorProfileName: "graph-committer", ActorProfileVersion: 1, SpecSnapshotJSON: []byte(`{"kind":"ActorProfile"}`)}}, []RunPhase{{Name: "warmup", Status: "pending", SpecSnapshotJSON: []byte(`{"name":"warmup"}`)}}); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := st.AppendEvent(ctx, RunEvent{RunID: runID, PhaseName: "warmup", EventType: "test", Payload: []byte(`{"ok":true}`)}); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if err := st.AppendMetric(ctx, RunMetric{RunID: runID, Name: "commits", Value: 1, Labels: []byte(`{"group":"writers"}`)}); err != nil {
		t.Fatalf("AppendMetric() error = %v", err)
	}
	size := int64(10)
	if err := st.AppendArtifact(ctx, RunArtifact{RunID: runID, Type: "log", Path: "artifacts/run/log.txt", SizeBytes: &size, Metadata: []byte(`{"source":"test"}`)}); err != nil {
		t.Fatalf("AppendArtifact() error = %v", err)
	}
}

func deleteRunForTest(ctx context.Context, st *PostgresStore, runID string) error {
	_, err := st.DB().ExecContext(ctx, `DELETE FROM test_runs WHERE id = $1`, runID)
	return err
}
