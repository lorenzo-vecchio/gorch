package gorch

import "context"

// HealthChecker is implemented by services that can report their own health.
// Health is called periodically by the orchestrator. A non-nil error means
// the service is unhealthy.
type HealthChecker interface {
	Health(ctx context.Context) error
}

// ReadinessChecker is implemented by services that distinguish "running" from
// "ready to serve". Ready returns nil when the service can accept traffic.
type ReadinessChecker interface {
	Ready(ctx context.Context) error
}

// Validator is implemented by services that validate their configuration at
// Register time. Validate is called immediately during Register; a non-nil
// error causes Register to return that error.
type Validator interface {
	Validate() error
}

// ServiceStatus represents the lifecycle state of a registered service.
type ServiceStatus int

const (
	StatusRegistered ServiceStatus = iota
	StatusStarting
	StatusRunning
	StatusStopping
	StatusStopped
	StatusCrashed
)

// String returns a human-readable name for the status.
func (s ServiceStatus) String() string {
	switch s {
	case StatusRegistered:
		return "registered"
	case StatusStarting:
		return "starting"
	case StatusRunning:
		return "running"
	case StatusStopping:
		return "stopping"
	case StatusStopped:
		return "stopped"
	case StatusCrashed:
		return "crashed"
	default:
		return "unknown"
	}
}
