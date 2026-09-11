package migrations

import "testing"

func TestAllMigrationsAreEmbeddedAndOrdered(t *testing.T) {
	migrations, err := All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("All() returned no migrations")
	}
	previous := ""
	for _, migration := range migrations {
		if migration.Version == "" || migration.Name == "" || migration.SQL == "" {
			t.Fatalf("migration has empty field: %+v", migration)
		}
		if previous != "" && migration.Version <= previous {
			t.Fatalf("migrations not ordered: %q before %q", previous, migration.Version)
		}
		previous = migration.Version
	}
}
