package koda

import (
	"strconv"
	"time"
)

type mockConn struct {
	keys   map[string]string
	lists  map[string][]string
	hashes map[string]map[string]string
	sets   map[string]map[string]float64 // map[member]score
}

func newConn() Conn {
	return &mockConn{
		keys:   make(map[string]string),
		hashes: make(map[string]map[string]string),
	}
}

func (c mockConn) Incr(key string) (int, error) {
	if _, ok := c.keys[key]; !ok {
		c.keys[key] = "0"
	}

	n, err := strconv.Atoi(c.keys[key])
	if err != nil {
		return 0, err
	}

	c.keys[key] = string(n + 1)
	return n + 1, nil
}

func (c mockConn) HGetAll(key string) ([]string, error) {
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

func (c mockConn) HSetAll(key string, fields map[string]string) error {
	if _, ok := c.hashes[key]; !ok {
		c.hashes[key] = make(map[string]string)
	}

	for k, v := range fields {
		c.hashes[key][k] = v
	}

	return nil
}

func (c mockConn) LPush(key string, value ...string) (int, error) {
	c.lists[key] = append(c.lists[key], value...)
	return len(c.lists[key]), nil
}

func (c mockConn) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
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

func (c mockConn) ZAddNX(key string, score float64, member string) (int, error) {
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
func (c mockConn) ZRem(key string, members ...string) (int, error) {
	if _, ok := c.sets[key]; !ok {
		return 0, nil
	}

	nRemoved := 0
	for _, m := range members {
		if _, ok := c.sets[key][m]; ok {
			delete(c.sets[key], m)
			nRemoved++
		}
	}

	return nRemoved, nil
}
func (c mockConn) ZRangeByScore(key string, opt ZRangeByScoreOpts) ([]string, error) {
	if _, ok := c.sets[key]; !ok {
		c.sets[key] = make(map[string]float64)
	}

	type Item struct {
		Member string
		Score  float64
	}

	var items []Item
	for m, s := range c.sets[key] {
		if (opt.MinInclusive && s < opt.Min) ||
			(!opt.MinInclusive && s <= opt.Min) ||
			(opt.MaxInclusive && s > opt.Max) ||
			(!opt.MaxInclusive && s >= opt.Min) {
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

	lo := opt.Offset
	if len(items) < lo {
		return nil, nil
	}
	hi := lo + opt.Count
	if hi > len(items) {
		hi = len(items)
	}
	items = items[lo:hi]

	var members []string
	for i := range items {
		members = append(members, items[i].Member)
	}

	return members, nil
}

func (c mockConn) Close() error { return nil }
