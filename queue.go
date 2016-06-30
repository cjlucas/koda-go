package koda

import "time"

const minPriority = 0
const maxPriority = 100

type Queue struct {
	Name          string
	NumWorkers    int
	MaxAttempts   int
	RetryInterval time.Duration
	queueKeys     []string
}
