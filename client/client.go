// Package client submits and inspects ForgeQ jobs over its authenticated HTTP API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const defaultMaxResponse = 2 << 20

var safeCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Options struct {
	BaseURL           string
	APIKey            string
	HTTPClient        *http.Client
	AllowInsecureHTTP bool
	MaxResponseBytes  int64
}

type Client struct {
	base        *url.URL
	apiKey      string
	http        *http.Client
	maxResponse int64
	retryBase   time.Duration
}

// APIError is a structured error returned by ForgeQ. Error excludes the
// server-provided message so credentials or payloads cannot enter logs.
type APIError struct {
	StatusCode               int
	Code, Message, RequestID string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("forgeq API error: status %d, code %s", e.StatusCode, e.Code)
}

type Job struct {
	TenantID        string          `json:"tenant_id"`
	ID              string          `json:"job_id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Status          string          `json:"status"`
	Priority        int             `json:"priority"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	ScheduledAt     time.Time       `json:"scheduled_at"`
	LeaseOwner      string          `json:"lease_owner,omitempty"`
	LeaseGeneration int64           `json:"lease_generation"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type SubmitRequest struct {
	Type           string `json:"type"`
	Payload        any    `json:"payload"`
	Priority       int    `json:"priority,omitempty"`
	MaxAttempts    int    `json:"max_attempts,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}
type ListOptions struct {
	Status, Type, Cursor string
	Limit                int
}
type JobPage struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"next_cursor"`
}
type JobType struct {
	Name            string `json:"name"`
	MaxPayloadBytes int    `json:"max_payload_bytes"`
}

// New creates a client without sending a request. HTTP is opt-in for local use.
func New(opts Options) (*Client, error) {
	u, err := url.Parse(opts.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("valid HTTP(S) base URL without credentials, query or fragment required")
	}
	if u.Scheme == "http" && !opts.AllowInsecureHTTP {
		return nil, errors.New("HTTP requires explicit local development opt-in")
	}
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, errors.New("API key is required")
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = defaultMaxResponse
	}
	if opts.MaxResponseBytes < 1 || opts.MaxResponseBytes > 16<<20 {
		return nil, errors.New("response limit must be between 1 byte and 16 MiB")
	}
	baseClient := opts.HTTPClient
	if baseClient == nil {
		baseClient = &http.Client{Timeout: 30 * time.Second}
	}
	copyClient := *baseClient
	// Never forward credentials or retry request bodies through a redirect.
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return &Client{base: u, apiKey: opts.APIKey, http: &copyClient, maxResponse: opts.MaxResponseBytes, retryBase: 250 * time.Millisecond}, nil
}

// Submit retries ambiguous failures only when the caller provides a stable key.
func (c *Client) Submit(ctx context.Context, request SubmitRequest) (Job, error) {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if len(request.IdempotencyKey) > 255 {
		return Job{}, errors.New("idempotency key exceeds 255 bytes")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return Job{}, errors.New("job request cannot be encoded")
	}
	var job Job
	err = c.do(ctx, http.MethodPost, "/v1/jobs", nil, body, request.IdempotencyKey, request.IdempotencyKey != "", &job)
	return job, err
}

func (c *Client) Get(ctx context.Context, id string) (Job, error) {
	var job Job
	path, err := jobPath(id)
	if err != nil {
		return job, err
	}
	err = c.do(ctx, http.MethodGet, path, nil, nil, "", true, &job)
	return job, err
}
func (c *Client) List(ctx context.Context, options ListOptions) (JobPage, error) {
	var page JobPage
	if options.Limit < 0 || options.Limit > 100 {
		return page, errors.New("limit must be between 1 and 100, or zero for default")
	}
	q := url.Values{}
	if options.Status != "" {
		q.Set("status", options.Status)
	}
	if options.Type != "" {
		q.Set("type", options.Type)
	}
	if options.Cursor != "" {
		q.Set("cursor", options.Cursor)
	}
	if options.Limit > 0 {
		q.Set("limit", strconv.Itoa(options.Limit))
	}
	err := c.do(ctx, http.MethodGet, "/v1/jobs", q, nil, "", true, &page)
	return page, err
}
func (c *Client) JobTypes(ctx context.Context) ([]JobType, error) {
	var result struct {
		JobTypes []JobType `json:"job_types"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/job-types", nil, nil, "", true, &result)
	return result.JobTypes, err
}
func (c *Client) Cancel(ctx context.Context, id string) (Job, error) {
	return c.mutate(ctx, id, "cancel")
}
func (c *Client) Retry(ctx context.Context, id string) (Job, error) {
	return c.mutate(ctx, id, "retry")
}
func (c *Client) mutate(ctx context.Context, id, action string) (Job, error) {
	var job Job
	path, err := jobPath(id)
	if err != nil {
		return job, err
	}
	err = c.do(ctx, http.MethodPost, path+"/"+action, nil, nil, "", false, &job)
	return job, err
}
func jobPath(id string) (string, error) {
	if !safeID.MatchString(id) {
		return "", errors.New("invalid job ID")
	}
	return "/v1/jobs/" + id, nil
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body []byte, key string, retry bool, out any) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	u := *c.base
	u.Path = c.base.Path + path
	if query != nil {
		u.RawQuery = query.Encode()
	}
	for attempt := 0; attempt < 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if err != nil {
			return errors.New("invalid request")
		}
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response, err := c.http.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if retry && attempt < 2 {
				if err = c.wait(ctx, attempt, 0); err != nil {
					return err
				}
				continue
			}
			return errors.New("forgeq request failed")
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, c.maxResponse+1))
		response.Body.Close()
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if retry && attempt < 2 {
				if err = c.wait(ctx, attempt, 0); err != nil {
					return err
				}
				continue
			}
			return errors.New("forgeq response could not be read")
		}
		if int64(len(data)) > c.maxResponse {
			return errors.New("forgeq response exceeds configured limit")
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			if err := json.Unmarshal(data, out); err != nil {
				return errors.New("forgeq response is invalid JSON")
			}
			return nil
		}
		apiErr := parseAPIError(response, data)
		if !retry || attempt == 2 || !retryable(response.StatusCode) {
			return apiErr
		}
		delay := retryAfter(response.Header.Get("Retry-After"), time.Now())
		if err := c.wait(ctx, attempt, delay); err != nil {
			return err
		}
	}
	return errors.New("forgeq request failed")
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status == http.StatusInternalServerError || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}
func parseAPIError(resp *http.Response, data []byte) *APIError {
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &envelope)
	code := envelope.Error.Code
	if !safeCode.MatchString(code) {
		code = "unknown_error"
	}
	return &APIError{StatusCode: resp.StatusCode, Code: code, Message: envelope.Error.Message, RequestID: resp.Header.Get("X-Request-ID")}
}
func retryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}
func (c *Client) wait(ctx context.Context, attempt int, minimum time.Duration) error {
	base := c.retryBase << attempt
	if base > 5*time.Second {
		base = 5 * time.Second
	}
	delay := time.Duration(float64(base) * (0.5 + rand.Float64()))
	if minimum > delay {
		delay = minimum
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
