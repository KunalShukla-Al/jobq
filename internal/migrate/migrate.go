// Package migrate applies the SQL files in migrations/ in order, once each.
package migrate

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var files embed.FS

// lockID is an arbitrary constant: every jobq instance takes the same
// advisory lock, so two starting at once apply migrations one after the other.
const lockID = 0x6a6f6271 // "jobq"

// Up applies every migration not yet recorded in jobq.schema_migrations.
func Up(ctx context.Context, pool *pgxpool.Pool) error {
	names, err := fs.Glob(files, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock($1)`, lockID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `create schema if not exists jobq;
			create table if not exists jobq.schema_migrations (
				version text primary key,
				applied_at timestamptz not null default now())`); err != nil {
			return err
		}
		for _, name := range names {
			var done bool
			if err := tx.QueryRow(ctx, `select exists (select 1 from jobq.schema_migrations where version = $1)`, name).Scan(&done); err != nil {
				return err
			}
			if done {
				continue
			}
			sql, err := files.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, `insert into jobq.schema_migrations (version) values ($1)`, name); err != nil {
				return err
			}
		}
		return nil
	})
}
