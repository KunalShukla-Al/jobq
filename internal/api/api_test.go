package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/KunalShukla-Al/jobq/internal/api"
	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
)

const secret = "s3cret"

func server(t *testing.T) *httptest.Server {
	t.Parallel()
	store := &queue.Store{Pool: testdb.New(t)}
	srv := httptest.NewServer((&api.Server{
		Store:   store,
		Secret:  secret,
		Kinds:   map[string]bool{"noop": true},
		Metrics: metrics.New(store),
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, auth, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func TestEnqueueAnswersByIdempotency(t *testing.T) {
	srv := server(t)
	job := `{"kind":"noop","payload":{"sleep_ms":5},"idempotency_key":"webpush:1234:class:2026-10-07T09:00"}`

	code, first := call(t, srv, "POST", "/jobs", secret, job)
	if code != http.StatusCreated || first["created"] != true {
		t.Fatalf("first: %d %v", code, first)
	}
	code, again := call(t, srv, "POST", "/jobs", secret, job)
	if code != http.StatusOK || again["created"] != false || again["id"] != first["id"] {
		t.Fatalf("repeat: %d %v, want 200 with id %v", code, again, first["id"])
	}
	code, _ = call(t, srv, "POST", "/jobs", secret, strings.Replace(job, `"sleep_ms":5`, `"sleep_ms":6`, 1))
	if code != http.StatusConflict {
		t.Fatalf("same key, different payload: %d, want 409", code)
	}
}

func TestEnqueueRejects(t *testing.T) {
	srv := server(t)
	for _, c := range []struct {
		name, auth, body string
		want             int
	}{
		{"no secret", "", `{"kind":"noop","idempotency_key":"k"}`, 401},
		{"wrong secret", "nope", `{"kind":"noop","idempotency_key":"k"}`, 401},
		{"not JSON", secret, `kind=noop`, 400},
		{"unknown field", secret, `{"kind":"noop","idempotency_key":"k","priority":1}`, 400},
		{"unknown kind", secret, `{"kind":"email","idempotency_key":"k"}`, 422},
		{"no idempotency key", secret, `{"kind":"noop"}`, 422},
		{"too many attempts", secret, `{"kind":"noop","idempotency_key":"k","max_attempts":1000}`, 422},
	} {
		t.Run(c.name, func(t *testing.T) {
			if code, body := call(t, srv, "POST", "/jobs", c.auth, c.body); code != c.want {
				t.Fatalf("%d %v, want %d", code, body, c.want)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	srv := server(t)
	_, made := call(t, srv, "POST", "/jobs", secret, `{"kind":"noop","idempotency_key":"k","max_attempts":3}`)
	id := int64(made["id"].(float64))

	code, job := call(t, srv, "GET", "/jobs/"+itoa(id), secret, "")
	if code != 200 || job["status"] != "ready" || job["max_attempts"] != float64(3) || job["idempotency_key"] != "k" {
		t.Fatalf("get: %d %v", code, job)
	}
	if _, leaked := job["lock_token"]; leaked {
		t.Fatal("the lease token must not be exposed")
	}
	if code, _ := call(t, srv, "GET", "/jobs/999", secret, ""); code != 404 {
		t.Fatalf("missing job: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/jobs/"+itoa(id), "", ""); code != 401 {
		t.Fatalf("no secret: %d", code)
	}
}

func TestHealthzNeedsNoSecret(t *testing.T) {
	srv := server(t)
	if code, body := call(t, srv, "GET", "/healthz", "", ""); code != 200 || body["status"] != "ok" {
		t.Fatalf("%d %v", code, body)
	}
}

func TestMetricsNeedNoSecretAndCountOnlyNewJobs(t *testing.T) {
	srv := server(t)
	job := `{"kind":"noop","idempotency_key":"once"}`
	call(t, srv, "POST", "/jobs", secret, job)
	call(t, srv, "POST", "/jobs", secret, job) // a repeat: not a new job

	res, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		t.Fatalf("/metrics: %d", res.StatusCode)
	}
	for _, want := range []string{
		`jobq_enqueued_total{kind="noop"} 1`,
		`jobq_queue_depth{status="ready"} 1`,
		`jobq_oldest_ready_age_seconds`,
		`go_goroutines`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
