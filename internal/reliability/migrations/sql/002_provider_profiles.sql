CREATE TABLE IF NOT EXISTS provider_profiles (
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

CREATE INDEX IF NOT EXISTS provider_profiles_name_version_idx ON provider_profiles (name, version DESC);
