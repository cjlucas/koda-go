package koda

import "time"

type ZRangeByScoreOpts struct {
	Min          float64
	Max          float64
	MinInclusive bool
	MaxInclusive bool
	Offset       int
	Count        int
}

type Conn interface {
	Incr(key string) (int, error)
	// TODO: Update this to return a map[string]string
	HGetAll(key string) ([]string, error)
	HSetAll(key string, fields map[string]string) error
	RPush(key string, value ...string) (int, error)
	BLPop(timeout time.Duration, keys ...string) ([]string, error)
	ZAddNX(key string, score float64, member string) (int, error)
	ZRem(key string, members ...string) (int, error)
	ZRangeByScore(key string, min, max float64, minIncl, maxIncl bool, offset, count int) ([]string, error)
	Close() error
}
