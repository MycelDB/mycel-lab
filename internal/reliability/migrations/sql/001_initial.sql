CREATE TABLE IF NOT EXISTS cluster_profiles (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    spec_hash TEXT NOT NULL,
    spec JSONB NOT NULL,
    resolved_spec JSONB,
    used_by_run_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE INDEX IF NOT EXISTS cluster_profiles_name_version_idx ON cluster_profiles (name, version DESC);

CREATE TABLE IF NOT EXISTS actor_profiles (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    spec_hash TEXT NOT NULL,
    spec JSONB NOT NULL,
    resolved_spec JSONB,
    used_by_run_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE INDEX IF NOT EXISTS actor_profiles_name_version_idx ON actor_profiles (name, version DESC);

CREATE TABLE IF NOT EXISTS scenarios (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    spec_hash TEXT NOT NULL,
    spec JSONB NOT NULL,
    resolved_spec JSONB,
    used_by_run_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE INDEX IF NOT EXISTS scenarios_name_version_idx ON scenarios (name, version DESC);

CREATE TABLE IF NOT EXISTS suites (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    spec_hash TEXT NOT NULL,
    spec JSONB NOT NULL,
    resolved_spec JSONB,
    used_by_run_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE INDEX IF NOT EXISTS suites_name_version_idx ON suites (name, version DESC);

CREATE TABLE IF NOT EXISTS test_runs (
    id TEXT PRIMARY KEY,
    scenario_name TEXT NOT NULL,
    scenario_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    seed BIGINT NOT NULL,
    resolved_spec JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS test_run_actor_profiles (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
    actor_group_name TEXT NOT NULL,
    actor_profile_name TEXT NOT NULL,
    actor_profile_version INTEGER NOT NULL,
    spec_snapshot JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS test_run_phases (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    spec_snapshot JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS test_run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
    event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    phase_name TEXT,
    event_type TEXT NOT NULL,
    actor_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS test_run_events_run_id_time_idx ON test_run_events (run_id, event_time, id);

CREATE TABLE IF NOT EXISTS test_run_metrics (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
    metric_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS test_run_metrics_run_id_name_time_idx ON test_run_metrics (run_id, name, metric_time);

CREATE TABLE IF NOT EXISTS test_run_artifacts (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    path TEXT NOT NULL,
    media_type TEXT,
    size_bytes BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS test_run_artifacts_run_id_idx ON test_run_artifacts (run_id);
