package migrations

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var migrationsFS embed.FS

// Migration represents a single database migration
type Migration struct {
	Version     int
	Description string
	SQL         string
}

// Migrator handles database migrations for the hook system
type Migrator struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewMigrator creates a new migrator
func NewMigrator(pool *pgxpool.Pool) *Migrator {
	return &Migrator{pool: pool, logger: slog.Default()}
}

// Migrate runs all pending migrations
func (m *Migrator) Migrate(ctx context.Context) error {
	if err := m.createMigrationsTable(ctx); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	currentVersion, err := m.getCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	migrations, err := m.loadMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	for _, migration := range migrations {
		if migration.Version > currentVersion {
			m.logger.Info("applying migration", "version", migration.Version, "description", migration.Description)

			tx, err := m.pool.Begin(ctx)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			if _, err := tx.Exec(ctx, migration.SQL); err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
			}

			if err := m.recordMigration(ctx, tx, migration); err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
			}

			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit migration %d: %w", migration.Version, err)
			}
		}
	}
	return nil
}

func (m *Migrator) createMigrationsTable(ctx context.Context) error {
	query := `
        CREATE TABLE IF NOT EXISTS pghook.migrations (
            version INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    `
	if _, err := m.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

func (m *Migrator) getCurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.pool.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM pghook.migrations").Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}
	return version, nil
}

func (m *Migrator) loadMigrations() ([]Migration, error) {
	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrations []Migration
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			version, description, err := parseMigrationFileName(entry.Name())
			if err != nil {
				return nil, err
			}

			content, err := migrationsFS.ReadFile(entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to read migration file %s: %w", entry.Name(), err)
			}

			migrations = append(migrations, Migration{
				Version:     version,
				Description: description,
				SQL:         string(content),
			})
		}
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	return migrations, nil
}

func (m *Migrator) recordMigration(ctx context.Context, tx pgx.Tx, migration Migration) error {
	query := "INSERT INTO pghook.migrations (version, description) VALUES ($1, $2)"
	_, err := tx.Exec(ctx, query, migration.Version, migration.Description)
	return err
}

func parseMigrationFileName(filename string) (version int, description string, err error) {
	// Expected format: v001_create_initial_schema.sql
	parts := strings.SplitN(strings.TrimSuffix(filename, ".sql"), "_", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid migration filename format: %s", filename)
	}

	versionStr := strings.TrimPrefix(parts[0], "v")
	version, err = strconv.Atoi(versionStr)
	if err != nil {
		return 0, "", fmt.Errorf("invalid version number in filename: %s", filename)
	}

	description = strings.ReplaceAll(parts[1], "_", " ")
	return version, description, nil
}
