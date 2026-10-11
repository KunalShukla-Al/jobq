package migrate_test

import (
	"context"
	"sync"
	"testing"

	"github.com/KunalShukla-Al/jobq/internal/migrate"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
)

// testdb.New has already migrated once; running again, from several
// instances at once, must change nothing and fail nowhere.
func TestUpIsIdempotentAndSafeToRunConcurrently(t *testing.T) {
	t.Parallel()
	pool := testdb.New(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- migrate.Up(context.Background(), pool) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var applied int
	if err := pool.QueryRow(context.Background(), `select count(*) from jobq.schema_migrations`).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("%d migrations recorded, want 1", applied)
	}
}
