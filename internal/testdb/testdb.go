// Package testdb gives each integration test its own empty, migrated
// Postgres database, so tests run in parallel without seeing each other's jobs.
//
// Set JOBQ_TEST_DATABASE_URL to a server the tests may create databases on,
// e.g. postgres://localhost/postgres. Without it, integration tests skip.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KunalShukla-Al/jobq/internal/migrate"
)

var n atomic.Int64

// New returns a pool on a fresh database, dropped when the test ends.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv("JOBQ_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("set JOBQ_TEST_DATABASE_URL to run integration tests against Postgres")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	name := fmt.Sprintf("jobq_t_%d_%d_%s", os.Getpid(), n.Add(1), strings.ToLower(sanitize(t.Name())))
	if len(name) > 60 {
		name = name[:60]
	}
	if _, err := admin.Exec(ctx, "create database "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	_ = admin.Close(ctx)

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse JOBQ_TEST_DATABASE_URL: %v", err)
	}
	u.Path = "/" + name
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if admin, err := pgx.Connect(ctx, base); err == nil {
			_, _ = admin.Exec(ctx, "drop database if exists "+pgx.Identifier{name}.Sanitize()+" with (force)")
			_ = admin.Close(ctx)
		}
	})
	return pool
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
