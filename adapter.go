package koda

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

var NilError = redis.Nil

// GoRedisAdapter is an adapter for the redis.v3 library
type GoRedisAdapter struct {
	R                 *redis.Client
	subscriptions     map[string][]chan string
	subscriptionsLock sync.RWMutex
}

func (r *GoRedisAdapter) Incr(key string) (int, error) {
	cmd := r.R.Incr(key)
	return int(cmd.Val()), cmd.Err()
}

func (r *GoRedisAdapter) HGetAll(key string) ([]string, error) {
	return r.R.HGetAll(key).Result()
}

func (r *GoRedisAdapter) HSetAll(key string, fields map[string]string) error {
	_, err := r.R.HMSetMap(key, fields).Result()
	return err
}

func (r *GoRedisAdapter) RPush(key string, value ...string) (int, error) {
	cmd := r.R.RPush(key, value...)
	return int(cmd.Val()), cmd.Err()
}

func (r *GoRedisAdapter) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return r.R.BLPop(timeout, keys...).Result()
}

func (r *GoRedisAdapter) ZAddNX(key string, score float64, member string) (int, error) {
	cmd := r.R.ZAddNX(key, redis.Z{
		Score:  score,
		Member: member,
	})

	return int(cmd.Val()), cmd.Err()
}

func (r *GoRedisAdapter) ZRem(key string, members ...string) (int, error) {
	cmd := r.R.ZRem(key, members...)
	return int(cmd.Val()), cmd.Err()
}

func (r *GoRedisAdapter) ZRangeByScore(key string, min, max float64, minIncl, maxIncl bool, offset, count int) ([]string, error) {
	var rangeStr [2]string
	ranges := []float64{min, max}
	inclusive := []bool{minIncl, maxIncl}

	for i := range ranges {
		rangeStr[i] = strconv.FormatFloat(ranges[i], 'E', -1, 64)
		if !inclusive[i] {
			rangeStr[i] = fmt.Sprintf("(%s", rangeStr[i])
		}
	}

	cmd := r.R.ZRangeByScore(key, redis.ZRangeByScore{
		Min:    rangeStr[0],
		Max:    rangeStr[1],
		Offset: int64(offset),
		Count:  int64(count),
	})

	return cmd.Val(), cmd.Err()
}

func (r *GoRedisAdapter) Scan(cursor int, match string, count int) (int, []string, error) {
	cmd := r.R.Scan(int64(cursor), match, int64(count))
	offset, results := cmd.Val()
	return int(offset), results, cmd.Err()
}

func (r *GoRedisAdapter) Subscribe(channel string) (<-chan string, error) {
	r.subscriptionsLock.Lock()
	defer r.subscriptionsLock.Unlock()
	if r.subscriptions == nil {
		r.subscriptions = make(map[string][]chan string)
	}
	ch := make(chan string)
	r.subscriptions[channel] = append(r.subscriptions[channel], ch)

	ps, err := r.R.Subscribe(channel)
	if err != nil {
		return nil, err
	}

	go func() {
		msg, _ := ps.ReceiveMessage()
		r.subscriptionsLock.Lock()
		for i := range r.subscriptions[channel] {
			r.subscriptions[channel][i] <- msg.Payload
		}
		r.subscriptionsLock.Unlock()
	}()

	return ch, nil
}

func (r *GoRedisAdapter) Close() error {
	return r.R.Close()
}
