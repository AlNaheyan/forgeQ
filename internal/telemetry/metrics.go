package telemetry

import (
	"forgequeue/internal/backup"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a service-local Prometheus registry. Keeping registries local
// avoids duplicate-registration panics in tests and lets Prometheus aggregate
// the same metric names from the API and coordinator processes.
type Metrics struct {
	TLSCertificateNotAfter                                                            *prometheus.GaugeVec
	DurableHealth, DurableQueueDepth, OldestReadyAge, OutboxLag, RetentionLastSuccess prometheus.Gauge
	RetentionDeleted                                                                  *prometheus.CounterVec
	RetentionErrors                                                                   prometheus.Counter
	registry                                                                          *prometheus.Registry

	JobsSubmitted          prometheus.Counter
	JobsStarted            prometheus.Counter
	JobsCompleted          prometheus.Counter
	JobsFailed             prometheus.Counter
	HandlerFailures        *prometheus.CounterVec
	JobsRetried            prometheus.Counter
	JobsDeadLetter         prometheus.Counter
	QueueDepth             prometheus.Gauge
	JobDuration            prometheus.Histogram
	JobQueueWait           prometheus.Histogram
	WorkersRegistered      prometheus.Gauge
	WorkersHealthy         prometheus.Gauge
	WorkersUnhealthy       prometheus.Gauge
	ActiveJobs             prometheus.Gauge
	LeaseExpirations       prometheus.Counter
	BackpressureRejections prometheus.Counter
	JobAttempts            prometheus.Histogram
	JobsAssigned           *prometheus.CounterVec
	APIRequestDuration     *prometheus.HistogramVec
}

type backupStatusCollector struct{ path string }

func (c backupStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range backupStatusDescs {
		ch <- d
	}
}
func (c backupStatusCollector) Collect(ch chan<- prometheus.Metric) {
	status, err := backup.ReadStatus(c.path)
	available := float64(0)
	if err == nil {
		available = 1
	}
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[0], prometheus.GaugeValue, available)
	if err != nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[1], prometheus.GaugeValue, float64(status.LastSuccess))
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[2], prometheus.GaugeValue, float64(status.LastSnapshot))
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[3], prometheus.GaugeValue, float64(status.ArchiveBytes))
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[4], prometheus.GaugeValue, status.DurationSeconds)
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[5], prometheus.CounterValue, float64(status.Successes), "success")
	ch <- prometheus.MustNewConstMetric(backupStatusDescs[5], prometheus.CounterValue, float64(status.Failures), "failure")
}

var backupStatusDescs = []*prometheus.Desc{
	prometheus.NewDesc("forgequeue_backup_status_available", "Whether the durable backup status file is readable.", nil, nil),
	prometheus.NewDesc("forgequeue_backup_last_success_timestamp_seconds", "Unix time of last completed encrypted upload.", nil, nil),
	prometheus.NewDesc("forgequeue_backup_last_snapshot_timestamp_seconds", "Unix time of last database snapshot start.", nil, nil),
	prometheus.NewDesc("forgequeue_backup_archive_bytes", "Size of the most recent completed encrypted archive.", nil, nil),
	prometheus.NewDesc("forgequeue_backup_duration_seconds", "Duration of the most recent backup attempt.", nil, nil),
	prometheus.NewDesc("forgequeue_backup_runs_total", "Backup operation attempts by outcome.", []string{"outcome"}, nil),
}

// SetBackupStatusFile adds a bounded local status-file collector. Missing or
// invalid files produce an explicit availability=0 metric for alerting.
func (m *Metrics) SetBackupStatusFile(path string) error {
	if path == "" {
		return nil
	}
	if err := m.registry.Register(backupStatusCollector{path: path}); err != nil {
		return err
	}
	return nil
}

// NewMetrics creates all ForgeQueue collectors and registers Go/process
// collectors so each service also exposes basic runtime health.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		TLSCertificateNotAfter: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "forgequeue_tls_certificate_not_after_timestamp_seconds", Help: "Expiry of loaded TLS certificate snapshot."}, []string{"endpoint", "role"}),
		DurableHealth:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_durable_health", Help: "Last durable operational snapshot succeeded (1/0)."}),
		DurableQueueDepth:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_durable_ready_jobs", Help: "Due queued jobs in PostgreSQL."}),
		OldestReadyAge:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_oldest_ready_seconds", Help: "Age of oldest due queued job."}),
		OutboxLag:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_outbox_oldest_pending_seconds", Help: "Age of oldest unpublished event."}),
		RetentionLastSuccess:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_retention_last_success_timestamp_seconds", Help: "Last committed enabled cleanup tick."}),
		RetentionDeleted:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "forgequeue_retention_deleted_total", Help: "Committed retention deletions by bounded parent kind."}, []string{"kind"}),
		RetentionErrors:        prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_retention_errors_total", Help: "Failed cleanup transactions."}),
		registry:               registry,
		JobsSubmitted:          prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_submitted_total", Help: "Jobs accepted by the API."}),
		JobsStarted:            prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_started_total", Help: "Job attempts started by workers."}),
		JobsCompleted:          prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_completed_total", Help: "Jobs completed successfully."}),
		JobsFailed:             prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_failed_total", Help: "Job attempts that failed."}),
		HandlerFailures:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "forgequeue_handler_failures_total", Help: "Failed job attempts by bounded classification."}, []string{"code"}),
		JobsRetried:            prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_retried_total", Help: "Failed jobs scheduled for retry."}),
		JobsDeadLetter:         prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_jobs_dead_letter_total", Help: "Jobs moved to the dead-letter state."}),
		QueueDepth:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_queue_depth", Help: "Jobs currently visible in the ready queue."}),
		JobDuration:            prometheus.NewHistogram(prometheus.HistogramOpts{Name: "forgequeue_job_duration_seconds", Help: "Job attempt execution duration.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 15)}),
		JobQueueWait:           prometheus.NewHistogram(prometheus.HistogramOpts{Name: "forgequeue_job_queue_wait_seconds", Help: "Time from job creation to attempt start.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 17)}),
		WorkersRegistered:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_workers_registered", Help: "Workers registered with the coordinator."}),
		WorkersHealthy:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_workers_healthy", Help: "Registered workers currently healthy."}),
		WorkersUnhealthy:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_workers_unhealthy", Help: "Registered workers currently unhealthy."}),
		ActiveJobs:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "forgequeue_active_jobs", Help: "Jobs currently assigned to workers."}),
		LeaseExpirations:       prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_lease_expirations_total", Help: "Expired leases recovered by the coordinator."}),
		BackpressureRejections: prometheus.NewCounter(prometheus.CounterOpts{Name: "forgequeue_backpressure_rejections_total", Help: "Submissions rejected by API backpressure."}),
		JobAttempts:            prometheus.NewHistogram(prometheus.HistogramOpts{Name: "forgequeue_job_attempts", Help: "Attempt number observed when a job starts.", Buckets: []float64{1, 2, 3, 4, 5, 8, 13, 21}}),
		JobsAssigned:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "forgequeue_jobs_assigned_total", Help: "Durable job leases assigned by worker."}, []string{"worker_id"}),
		APIRequestDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "forgequeue_api_request_duration_seconds", Help: "HTTP API request duration.", Buckets: prometheus.DefBuckets}, []string{"method", "route", "status"}),
	}
	registry.MustRegister(m.TLSCertificateNotAfter,
		m.DurableHealth, m.DurableQueueDepth, m.OldestReadyAge, m.OutboxLag, m.RetentionLastSuccess, m.RetentionDeleted, m.RetentionErrors,
		m.JobsSubmitted, m.JobsStarted, m.JobsCompleted, m.JobsFailed, m.HandlerFailures,
		m.JobsRetried, m.JobsDeadLetter, m.QueueDepth, m.JobDuration,
		m.JobQueueWait, m.WorkersRegistered, m.WorkersHealthy,
		m.WorkersUnhealthy, m.ActiveJobs, m.LeaseExpirations,
		m.BackpressureRejections, m.JobAttempts, m.JobsAssigned,
		m.APIRequestDuration, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return m
}

// Handler returns a Prometheus exposition handler for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

// Gatherer is exposed for focused tests and tooling.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.registry }

// RecordBackpressureRejection implements jobs.BackpressureObserver without
// coupling the telemetry package to the jobs package.
func (m *Metrics) RecordBackpressureRejection() { m.BackpressureRejections.Inc() }

// RecordHandlerFailure maps legacy or arbitrary error text to a bounded label.
func (m *Metrics) RecordHandlerFailure(code string) {
	switch code {
	case "invalid_payload", "unsupported_type", "permanent", "result_invalid", "panic", "downstream_unavailable", "rate_limited", "timeout", "unclassified":
	default:
		code = "unclassified"
	}
	m.HandlerFailures.WithLabelValues(code).Inc()
}

// ObserveAPIRequest records an HTTP request using bounded-cardinality labels.
func (m *Metrics) ObserveAPIRequest(method, route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	m.APIRequestDuration.WithLabelValues(method, route, strconv.Itoa(status)).Observe(duration.Seconds())
}
