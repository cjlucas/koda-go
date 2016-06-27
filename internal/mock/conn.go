package mock

import (
	"strconv"
	"sync"
	"time"
)

type Conn struct {
	keys   map[string]string
	lists  map[string][]string
	hashes map[string]map[string]string
	sets   map[string]map[string]float64 // map[member]score
	lock   sync.RWMutex
}

func NewConn() *Conn {
	return &Conn{
		keys:   make(map[string]string),
		lists:  make(map[string][]string),
		hashes: make(map[string]map[string]string),
		sets:   make(map[string]map[string]float64),
	}
}

func (c *Conn) Incr(key string) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.keys[key]; !ok {
		c.keys[key] = "0"
	}

	n, err := strconv.Atoi(c.keys[key])
	if err != nil {
		return 0, err
	}

	c.keys[key] = strconv.Itoa(n + 1)
	return n + 1, nil
}

func (c *Conn) HGetAll(key string) ([]string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	hash, ok := c.hashes[key]
	if !ok {
		return nil, nil
	}

	pairs := make([]string, len(hash)*2)
	for k, v := range hash {
		pairs = append(pairs, k)
		pairs = append(pairs, v)
	}

	return pairs, nil
}

func (c *Conn) HSetAll(key string, fields map[string]string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.hashes[key]; !ok {
		c.hashes[key] = make(map[string]string)
	}

	for k, v := range fields {
		c.hashes[key][k] = v
	}

	return nil
}

func (c *Conn) RPush(key string, value ...string) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.lists[key] = append(c.lists[key], value...)
	return len(c.lists[key]), nil
}

func (c *Conn) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// TODO: add support for timeouts?
	for _, key := range keys {
		if len(c.lists[key]) > 0 {
			v := c.lists[key][0]
			c.lists[key] = c.lists[key][1:]
			return []string{key, v}, nil
		}
	}

	return nil, nil
}

func (c *Conn) ZAddNX(key string, score float64, member string) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// handle the NX part
	if set, ok := c.sets[key]; ok {
		if _, ok := set[member]; ok {
			return 0, nil
		}
	}

	if _, ok := c.sets[key]; !ok {
		c.sets[key] = make(map[string]float64)
	}

	c.sets[key][member] = score
	return 1, nil
}

func (c *Conn) ZPopByScore(key string, min, max float64, minIncl, maxIncl bool, offset, count int) ([]string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.sets[key]; !ok {
		c.sets[key] = make(map[string]float64)
	}

	type Item struct {
		Member string
		Score  float64
	}

	var items []Item
	for m, s := range c.sets[key] {
		if (minIncl && s < min) ||
			(!minIncl && s <= min) ||
			(maxIncl && s > max) ||
			(!maxIncl && s >= max) {
			continue
		}

		pos := 0
		for i := range items {
			if s > items[i].Score {
				pos = i
				break
			}
		}

		item := Item{Member: m, Score: s}
		items = append(items[:pos], append([]Item{item}, items[pos:]...)...)
	}

	lo := offset
	if len(items) < lo {
		return nil, nil
	}
	hi := lo + count
	if hi > len(items) {
		hi = len(items)
	}
	items = items[lo:hi]

	var members []string
	for i := range items {
		member := items[i].Member
		members = append(members, member)
		delete(c.sets[key], member)
	}

	return members, nil
}

func (c *Conn) Subscribe(channel string) (<-chan string, error) {
	return nil, nil
}

func (c *Conn) Close() error { return nil }
