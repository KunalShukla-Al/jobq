// Package api is jobq's HTTP interface: enqueue a job, look one up, health.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// Server serves the API.
type Server struct {
	Store  queue.Backend
	Secret string          // required as "Authorization: Bearer <secret>"
	Kinds  map[string]bool // job kinds that may be enqueued
	// Metrics, when set, counts new jobs and is served at /metrics. Like
	// /healthz it needs no secret: it holds counts and timings, never payloads.
	Metrics *metrics.Metrics
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
	}
	mux.Handle("POST /jobs", s.auth(http.HandlerFunc(s.enqueue)))
	mux.Handle("GET /jobs/{id}", s.auth(http.HandlerFunc(s.get)))
	return mux
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if s.Secret == "" || !ok || subtle.ConstantTimeCompare([]byte(got), []byte(s.Secret)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or wrong secret")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type enqueueRequest struct {
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	RunAt          *time.Time      `json:"run_at"`
	MaxAttempts    int             `json:"max_attempts"`
}

type enqueueResponse struct {
	ID      int64 `json:"id"`
	Created bool  `json:"created"`
}

// enqueue: 201 for a new job, 200 for the existing one with the same key,
// 409 if that key was used for a different job.
func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON job: "+err.Error())
		return
	}
	switch {
	case !s.Kinds[req.Kind]:
		writeError(w, http.StatusUnprocessableEntity, "unknown kind "+strconv.Quote(req.Kind))
		return
	case req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200:
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key is required (at most 200 characters)")
		return
	case req.MaxAttempts < 0 || req.MaxAttempts > 100:
		writeError(w, http.StatusUnprocessableEntity, "max_attempts must be between 1 and 100 (or left out for 8)")
		return
	}
	n := queue.NewJob{Kind: req.Kind, Payload: req.Payload, IdempotencyKey: req.IdempotencyKey, MaxAttempts: req.MaxAttempts}
	if req.RunAt != nil {
		n.RunAt = *req.RunAt
	}

	id, created, err := s.Store.Enqueue(r.Context(), n)
	switch {
	case errors.Is(err, queue.ErrKeyConflict):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "could not enqueue")
	case created:
		s.Metrics.Enqueued(req.Kind)
		writeJSON(w, http.StatusCreated, enqueueResponse{ID: id, Created: true})
	default:
		writeJSON(w, http.StatusOK, enqueueResponse{ID: id, Created: false})
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a number")
		return
	}
	j, err := s.Store.Get(r.Context(), id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, "no such job")
	case err != nil:
		writeError(w, http.StatusInternalServerError, "could not read the job")
	default:
		writeJSON(w, http.StatusOK, j)
	}
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
