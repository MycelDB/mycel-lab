package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed sql/*.sql
var files embed.FS

type Migration struct {
	Version string
	Name    string
	SQL     string
}

type Status struct {
	Version string
	Name    string
	Applied bool
}

func All() ([]Migration, error) {
	entries, err := fs.ReadDir(files, "sql")
	if err != nil {
		return nil, err
	}
	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		content, err := files.ReadFile("sql/" + entry.Name())
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, Migration{Version: version, Name: entry.Name(), SQL: string(content)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, nil
}

func Apply(ctx context.Context, db *sql.DB) error {
	if err := ensureSchemaTable(ctx, db); err != nil {
		return err
	}
	migrations, err := All()
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		applied, err := isApplied(ctx, db, migration.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := applyOne(ctx, db, migration); err != nil {
			return err
		}
	}
	return nil
}

func GetStatus(ctx context.Context, db *sql.DB) ([]Status, error) {
	if err := ensureSchemaTable(ctx, db); err != nil {
		return nil, err
	}
	migrations, err := All()
	if err != nil {
		return nil, err
	}
	statuses := make([]Status, 0, len(migrations))
	for _, migration := range migrations {
		applied, err := isApplied(ctx, db, migration.Version)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, Status{Version: migration.Version, Name: migration.Name, Applied: applied})
	}
	return statuses, nil
}

func ensureSchemaTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS reliability_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	return err
}

func isApplied(ctx context.Context, db *sql.DB, version string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM reliability_schema_migrations WHERE version = $1)`, version).Scan(&exists)
	return exists, err
}

func applyOne(ctx context.Context, db *sql.DB, migration Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, migration.SQL); err != nil {
		return fmt.Errorf("apply migration %s: %w", migration.Name, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO reliability_schema_migrations (version) VALUES ($1)`, migration.Version); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	return tx.Commit()
}
