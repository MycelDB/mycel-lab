package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/MycelDB/mycel-lab/internal/reliability/migrations"
)

var ErrNotFound = errors.New("not found")
var ErrDefinitionInUse = errors.New("definition is used by a run")

type PostgresStore struct {
	db *sql.DB
}

func OpenPostgres(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

func (s *PostgresStore) DB() *sql.DB { return s.db }

func (s *PostgresStore) Migrate(ctx context.Context) error {
	return migrations.Apply(ctx, s.db)
}

func (s *PostgresStore) MigrationStatus(ctx context.Context) ([]migrations.Status, error) {
	return migrations.GetStatus(ctx, s.db)
}

func (s *PostgresStore) UpsertDefinition(ctx context.Context, def Definition) (UpsertResult, error) {
	table, err := tableForKind(def.Kind)
	if err != nil {
		return UpsertResult{}, err
	}
	latest, err := s.LatestDefinition(ctx, def.Kind, def.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return UpsertResult{}, err
	}
	if errors.Is(err, ErrNotFound) {
		def.Version = 1
		created, err := s.insertDefinition(ctx, table, def)
		return UpsertResult{Definition: created, Status: UpsertCreated}, err
	}
	if latest.SpecHash == def.SpecHash {
		return UpsertResult{Definition: latest, Status: UpsertNoop}, nil
	}
	if latest.CanEdit() {
		latest.SpecHash = def.SpecHash
		latest.SpecJSON = def.SpecJSON
		latest.ResolvedJSON = def.ResolvedJSON
		updated, err := s.updateDefinition(ctx, table, latest)
		return UpsertResult{Definition: updated, Status: UpsertUpdated}, err
	}
	def.Version = latest.Version + 1
	created, err := s.insertDefinition(ctx, table, def)
	return UpsertResult{Definition: created, Status: UpsertCreated}, err
}

func (s *PostgresStore) ListDefinitions(ctx context.Context, kind DefinitionKind) ([]Definition, error) {
	table, err := tableForKind(kind)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT id, name, version, spec_hash, spec, COALESCE(resolved_spec, 'null'::jsonb), used_by_run_count, created_at, updated_at FROM %s ORDER BY name, version`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Definition
	for rows.Next() {
		def, err := scanDefinition(rows, kind)
		if err != nil {
			return nil, err
		}
		out = append(out, def)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetDefinition(ctx context.Context, kind DefinitionKind, name string, version int) (Definition, error) {
	table, err := tableForKind(kind)
	if err != nil {
		return Definition{}, err
	}
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT id, name, version, spec_hash, spec, COALESCE(resolved_spec, 'null'::jsonb), used_by_run_count, created_at, updated_at FROM %s WHERE name = $1 AND version = $2`, table), name, version)
	return scanDefinition(row, kind)
}

func (s *PostgresStore) LatestDefinition(ctx context.Context, kind DefinitionKind, name string) (Definition, error) {
	table, err := tableForKind(kind)
	if err != nil {
		return Definition{}, err
	}
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT id, name, version, spec_hash, spec, COALESCE(resolved_spec, 'null'::jsonb), used_by_run_count, created_at, updated_at FROM %s WHERE name = $1 ORDER BY version DESC LIMIT 1`, table), name)
	return scanDefinition(row, kind)
}

func (s *PostgresStore) DeleteDefinition(ctx context.Context, kind DefinitionKind, name string, version int) error {
	table, err := tableForKind(kind)
	if err != nil {
		return err
	}
	def, err := s.GetDefinition(ctx, kind, name, version)
	if err != nil {
		return err
	}
	if !def.CanDelete() {
		return ErrDefinitionInUse
	}
	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE name = $1 AND version = $2`, table), name, version)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) MarkDefinitionUsed(ctx context.Context, kind DefinitionKind, name string, version int) error {
	table, err := tableForKind(kind)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET used_by_run_count = used_by_run_count + 1, updated_at = now() WHERE name = $1 AND version = $2`, table), name, version)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) CreateRun(ctx context.Context, run Run, actors []RunActorProfile, phases []RunPhase) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO test_runs (id, scenario_name, scenario_version, status, seed, resolved_spec) VALUES ($1, $2, $3, $4, $5, $6)`, run.ID, run.ScenarioName, run.ScenarioVersion, run.Status, run.Seed, jsonOrObject(run.ResolvedJSON)); err != nil {
		return err
	}
	for _, actor := range actors {
		if _, err := tx.ExecContext(ctx, `INSERT INTO test_run_actor_profiles (run_id, actor_group_name, actor_profile_name, actor_profile_version, spec_snapshot) VALUES ($1, $2, $3, $4, $5)`, run.ID, actor.ActorGroupName, actor.ActorProfileName, actor.ActorProfileVersion, jsonOrObject(actor.SpecSnapshotJSON)); err != nil {
			return err
		}
	}
	for _, phase := range phases {
		if _, err := tx.ExecContext(ctx, `INSERT INTO test_run_phases (run_id, name, status, spec_snapshot) VALUES ($1, $2, $3, $4)`, run.ID, phase.Name, phase.Status, jsonOrObject(phase.SpecSnapshotJSON)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) FinishRun(ctx context.Context, runID string, status string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE test_runs SET status = $2, finished_at = now() WHERE id = $1`, runID, status)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, scenario_name, scenario_version, status, seed, resolved_spec, started_at, finished_at, created_at FROM test_runs ORDER BY started_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *PostgresStore) GetRun(ctx context.Context, runID string) (RunDetails, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx, `SELECT id, scenario_name, scenario_version, status, seed, resolved_spec, started_at, finished_at, created_at FROM test_runs WHERE id = $1`, runID))
	if err != nil {
		return RunDetails{}, err
	}
	details := RunDetails{Run: run}
	eventRows, err := s.db.QueryContext(ctx, `SELECT run_id, COALESCE(phase_name, ''), event_type, COALESCE(actor_id, ''), payload FROM test_run_events WHERE run_id = $1 ORDER BY event_time, id`, runID)
	if err != nil {
		return RunDetails{}, err
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var event RunEvent
		if err := eventRows.Scan(&event.RunID, &event.PhaseName, &event.EventType, &event.ActorID, &event.Payload); err != nil {
			return RunDetails{}, err
		}
		details.Events = append(details.Events, event)
	}
	if err := eventRows.Err(); err != nil {
		return RunDetails{}, err
	}
	metricRows, err := s.db.QueryContext(ctx, `SELECT run_id, name, value, labels FROM test_run_metrics WHERE run_id = $1 ORDER BY metric_time, id`, runID)
	if err != nil {
		return RunDetails{}, err
	}
	defer metricRows.Close()
	for metricRows.Next() {
		var metric RunMetric
		if err := metricRows.Scan(&metric.RunID, &metric.Name, &metric.Value, &metric.Labels); err != nil {
			return RunDetails{}, err
		}
		details.Metrics = append(details.Metrics, metric)
	}
	if err := metricRows.Err(); err != nil {
		return RunDetails{}, err
	}
	artifactRows, err := s.db.QueryContext(ctx, `SELECT run_id, artifact_type, path, COALESCE(media_type, ''), size_bytes, metadata FROM test_run_artifacts WHERE run_id = $1 ORDER BY created_at, id`, runID)
	if err != nil {
		return RunDetails{}, err
	}
	defer artifactRows.Close()
	for artifactRows.Next() {
		var artifact RunArtifact
		if err := artifactRows.Scan(&artifact.RunID, &artifact.Type, &artifact.Path, &artifact.MediaType, &artifact.SizeBytes, &artifact.Metadata); err != nil {
			return RunDetails{}, err
		}
		details.Artifacts = append(details.Artifacts, artifact)
	}
	return details, artifactRows.Err()
}

func (s *PostgresStore) AppendEvent(ctx context.Context, event RunEvent) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO test_run_events (run_id, phase_name, event_type, actor_id, payload) VALUES ($1, NULLIF($2, ''), $3, NULLIF($4, ''), $5)`, event.RunID, event.PhaseName, event.EventType, event.ActorID, jsonOrObject(event.Payload))
	return err
}

func (s *PostgresStore) AppendMetric(ctx context.Context, metric RunMetric) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO test_run_metrics (run_id, name, value, labels) VALUES ($1, $2, $3, $4)`, metric.RunID, metric.Name, metric.Value, jsonOrObject(metric.Labels))
	return err
}

func (s *PostgresStore) AppendArtifact(ctx context.Context, artifact RunArtifact) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO test_run_artifacts (run_id, artifact_type, path, media_type, size_bytes, metadata) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)`, artifact.RunID, artifact.Type, artifact.Path, artifact.MediaType, artifact.SizeBytes, jsonOrObject(artifact.Metadata))
	return err
}

func (s *PostgresStore) insertDefinition(ctx context.Context, table string, def Definition) (Definition, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`INSERT INTO %s (name, version, spec_hash, spec, resolved_spec) VALUES ($1, $2, $3, $4, NULLIF($5, 'null')::jsonb) RETURNING id, name, version, spec_hash, spec, COALESCE(resolved_spec, 'null'::jsonb), used_by_run_count, created_at, updated_at`, table), def.Name, def.Version, def.SpecHash, jsonOrObject(def.SpecJSON), jsonOrNull(def.ResolvedJSON))
	return scanDefinition(row, def.Kind)
}

func (s *PostgresStore) updateDefinition(ctx context.Context, table string, def Definition) (Definition, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`UPDATE %s SET spec_hash = $3, spec = $4, resolved_spec = NULLIF($5, 'null')::jsonb, updated_at = now() WHERE name = $1 AND version = $2 RETURNING id, name, version, spec_hash, spec, COALESCE(resolved_spec, 'null'::jsonb), used_by_run_count, created_at, updated_at`, table), def.Name, def.Version, def.SpecHash, jsonOrObject(def.SpecJSON), jsonOrNull(def.ResolvedJSON))
	return scanDefinition(row, def.Kind)
}

func tableForKind(kind DefinitionKind) (string, error) {
	switch kind {
	case KindClusterProfile:
		return "cluster_profiles", nil
	case KindActorProfile:
		return "actor_profiles", nil
	case KindScenario:
		return "scenarios", nil
	case KindSuite:
		return "suites", nil
	default:
		return "", fmt.Errorf("unsupported definition kind %q", kind)
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRun(row scanner) (Run, error) {
	var run Run
	if err := row.Scan(&run.ID, &run.ScenarioName, &run.ScenarioVersion, &run.Status, &run.Seed, &run.ResolvedJSON, &run.StartedAt, &run.FinishedAt, &run.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, ErrNotFound
		}
		return Run{}, err
	}
	return run, nil
}

func scanDefinition(row scanner, kind DefinitionKind) (Definition, error) {
	var def Definition
	def.Kind = kind
	if err := row.Scan(&def.ID, &def.Name, &def.Version, &def.SpecHash, &def.SpecJSON, &def.ResolvedJSON, &def.UsedByRunCount, &def.CreatedAt, &def.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Definition{}, ErrNotFound
		}
		return Definition{}, err
	}
	return def, nil
}

func jsonOrObject(raw []byte) string {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return `{}`
	}
	return string(raw)
}

func jsonOrNull(raw []byte) string {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return `null`
	}
	return string(raw)
}
