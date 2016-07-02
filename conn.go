package koda

import "time"

// Conn may be implemented by custom redis connections. See Options.ConnFactory.
// Note to implementers, each function must be atomic.
type Conn interface {
	Incr(key string) (int, error)
	// TODO: Update this to return a map[string]string
	HGetAll(key string) ([]string, error)
	HSetAll(key string, fields map[string]string) error
	RPush(key string, value ...string) (int, error)
	BLPop(timeout time.Duration, keys ...string) ([]string, error)
	ZAddNX(key string, score float64, member string) (int, error)
	// ZPopByScore has the same interface as ZRANGEBYSCORE, but also removes each member
	ZPopByScore(key string, min, max float64, minIncl, maxIncl bool, offset, count int) ([]string, error)
	Subscribe(channel string) (<-chan string, error)
	Close() error
}
