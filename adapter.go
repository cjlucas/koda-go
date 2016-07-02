package koda

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

// redisAdapter is an adapter for the redis.v3 library
type redisAdapter struct {
	R                 *redis.Client
	subscriptions     map[string][]chan string
	subscriptionsLock sync.RWMutex
}

func (r *redisAdapter) Incr(key string) (int, error) {
	cmd := r.R.Incr(key)
	return int(cmd.Val()), cmd.Err()
}

func (r *redisAdapter) HGetAll(key string) ([]string, error) {
	return r.R.HGetAll(key).Result()
}

func (r *redisAdapter) HSetAll(key string, fields map[string]string) error {
	_, err := r.R.HMSetMap(key, fields).Result()
	return err
}

func (r *redisAdapter) RPush(key string, value ...string) (int, error) {
	cmd := r.R.RPush(key, value...)
	return int(cmd.Val()), cmd.Err()
}

func (r *redisAdapter) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return r.R.BLPop(timeout, keys...).Result()
}

func (r *redisAdapter) ZAddNX(key string, score float64, member string) (int, error) {
	cmd := r.R.ZAddNX(key, redis.Z{
		Score:  score,
		Member: member,
	})

	return int(cmd.Val()), cmd.Err()
}

func (r *redisAdapter) ZPopByScore(key string, min, max float64, minIncl, maxIncl bool, offset, count int) ([]string, error) {
	script := `
	local res = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])
	for i=1,#res do
		redis.call('ZREM', KEYS[1], res[i])
	end
	return res
	`
	var rangeStr [2]string
	ranges := []float64{min, max}
	inclusive := []bool{minIncl, maxIncl}

	for i := range ranges {
		rangeStr[i] = strconv.FormatFloat(ranges[i], 'E', -1, 64)
		if !inclusive[i] {
			rangeStr[i] = fmt.Sprintf("(%s", rangeStr[i])
		}
	}

	cmd := r.R.Eval(script, []string{key}, []string{
		rangeStr[0],
		rangeStr[1],
		strconv.Itoa(offset),
		strconv.Itoa(count),
	})

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	var members []string
	val := cmd.Val().([]interface{})
	for i := range val {
		members = append(members, val[i].(string))
	}

	return members, nil
}

func (r *redisAdapter) Scan(cursor int, match string, count int) (int, []string, error) {
	cmd := r.R.Scan(int64(cursor), match, int64(count))
	offset, results := cmd.Val()
	return int(offset), results, cmd.Err()
}

func (r *redisAdapter) Subscribe(channel string) (<-chan string, error) {
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

func (r *redisAdapter) Close() error {
	return r.R.Close()
}
