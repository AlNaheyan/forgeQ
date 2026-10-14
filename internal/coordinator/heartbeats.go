package coordinator

import (
	"errors"
	"time"
)

// Health describes the coordinator's current view of a worker.
type Health string

const (
	HealthHealthy   Health = "healthy"
	HealthSuspect   Health = "suspect"
	HealthUnhealthy Health = "unhealthy"
)

// HealthThresholds controls when a worker moves from healthy to suspect and
// from suspect to unhealthy. The boundary is inclusive: a worker whose last
// contact was exactly SuspectAfter ago is suspect.
type HealthThresholds struct {
	SuspectAfter   time.Duration
	UnhealthyAfter time.Duration
}

func (t HealthThresholds) validate() error {
	if t.SuspectAfter <= 0 {
		return errors.New("suspect threshold must be greater than zero")
	}
	if t.UnhealthyAfter <= t.SuspectAfter {
		return errors.New("unhealthy threshold must be greater than suspect threshold")
	}
	return nil
}

func (t HealthThresholds) health(now, lastSeen time.Time) Health {
	age := now.Sub(lastSeen)
	switch {
	case age >= t.UnhealthyAfter:
		return HealthUnhealthy
	case age >= t.SuspectAfter:
		return HealthSuspect
	default:
		return HealthHealthy
	}
}
