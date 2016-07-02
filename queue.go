package koda

import "time"

const minPriority = 0
const maxPriority = 100

// Queue represesents a configurable queue.
type Queue struct {
	Name string

	// The number of simultaneous workers
	// Default: 1
	NumWorkers int

	// The maximum number of attempts for a single job.
	// Default: 1
	MaxAttempts int

	// The interval between attempts
	// Default: 0
	RetryInterval time.Duration

	queueKeys []string
}
