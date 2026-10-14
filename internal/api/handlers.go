package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"forgequeue/internal/jobs"
)

const maxRequestBody = 1 << 20

type Server struct {
	jobs   *jobs.Service
	ready  func(*http.Request) error
	logger *slog.Logger
}

func NewServer(service *jobs.Service, ready func(*http.Request) error, logger *slog.Logger) *Server {
	return &Server{jobs: service, ready: ready, logger: logger}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.createJob)
	mux.HandleFunc("GET /v1/jobs", s.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.readiness)
	return s.recoverPanics(mux)
}

type createJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var request createJobRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if headerKey != "" && request.IdempotencyKey != "" && headerKey != strings.TrimSpace(request.IdempotencyKey) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key header and idempotency_key body field must match")
		return
	}
	if headerKey != "" {
		request.IdempotencyKey = headerKey
	}

	result, err := s.jobs.Submit(r.Context(), jobs.CreateInput{
		Type:           request.Type,
		Payload:        request.Payload,
		Priority:       request.Priority,
		MaxAttempts:    request.MaxAttempts,
		IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	if result.EnqueueError != nil {
		s.logger.Warn("job persisted but enqueue failed; reconciliation will retry",
			"job_id", result.Job.ID,
			"error", result.EnqueueError,
		)
	}

	status := http.StatusCreated
	if !result.Created {
		status = http.StatusOK
	}
	writeJSON(w, status, result.Job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.jobs.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	filter := jobs.ListFilter{
		Status: jobs.Status(r.URL.Query().Get("status")),
		Type:   r.URL.Query().Get("type"),
		Cursor: r.URL.Query().Get("cursor"),
		Limit:  50,
	}
	if filter.Status != "" && !jobs.ValidStatus(filter.Status) {
		writeError(w, http.StatusBadRequest, "invalid_request", "unknown status filter")
		return
	}
	if filter.Type != "" && !jobs.SupportedType(filter.Type) {
		writeError(w, http.StatusBadRequest, "invalid_request", "unknown type filter")
		return
	}
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100")
			return
		}
		filter.Limit = limit
	}

	listed, nextCursor, err := s.jobs.List(r.Context(), filter)
	if err != nil {
		if strings.Contains(err.Error(), "cursor") {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid cursor")
			return
		}
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":        listed,
		"next_cursor": nextCursor,
	})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.jobs.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		if err := s.ready(r); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "database is unavailable")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrInvalidJob):
		writeError(w, http.StatusBadRequest, "invalid_job", err.Error())
	case errors.Is(err, jobs.ErrNotFound):
		writeError(w, http.StatusNotFound, "job_not_found", err.Error())
	case errors.Is(err, jobs.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_transition", err.Error())
	default:
		s.logger.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("request panic", "error", fmt.Sprint(recovered))
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
