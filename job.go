package koda

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type JobState int

const (
	Initial  JobState = 0
	Queued            = 1
	Working           = 2
	Finished          = 3
	Dead              = 4
)

func (s JobState) String() string {
	switch s {
	case Initial:
		return "Initial"
	case Queued:
		return "Queued"
	case Working:
		return "Working"
	case Finished:
		return "Finished"
	case Dead:
		return "Dead"
	default:
		panic(fmt.Sprintf("Unknown state: %d", s))
	}
}

type Job struct {
	ID             int
	State          JobState
	DelayedUntil   time.Time
	CreationTime   time.Time
	CompletionTime time.Time
	Priority       int
	Payload        interface{}
	rawPayload     string
	NumAttempts    int
}

func (j *Job) hash() (map[string]string, error) {
	hash := map[string]string{
		"id":              strconv.Itoa(int(j.ID)),
		"state":           strconv.Itoa(int(j.State)),
		"delayed_until":   strconv.Itoa(int(j.DelayedUntil.Unix())),
		"creation_time":   strconv.Itoa(int(j.CreationTime.Unix())),
		"completion_time": strconv.Itoa(int(j.CompletionTime.Unix())),
		"priority":        strconv.Itoa(int(j.Priority)),
		"num_attempts":    strconv.Itoa(int(j.NumAttempts)),
	}

	jsonPayload, err := json.Marshal(j.Payload)
	if err != nil {
		return nil, err
	}

	hash["payload"] = string(jsonPayload)
	return hash, nil
}

func (j *Job) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal([]byte(j.rawPayload), v)
}

type jobUnmarshaller struct {
	Err error
}

func (u *jobUnmarshaller) atoi(s string) int {
	if u.Err != nil || s == "" {
		return 0
	}

	val, err := strconv.Atoi(s)
	if err != nil {
		u.Err = err
	}

	return val
}

func (u *jobUnmarshaller) parseJSON(s string) interface{} {
	if u.Err != nil || s == "" {
		return nil
	}

	var val interface{}
	if err := json.Unmarshal([]byte(s), &val); err != nil {
		u.Err = err
	}

	return val
}

func (u *jobUnmarshaller) atot(s string) time.Time {
	secs := u.atoi(s)
	if u.Err != nil {
		return time.Time{}
	}

	return time.Unix(int64(secs), 0).UTC()
}

func unmarshalJob(c Conn, key string) (*Job, error) {
	propMap := make(map[string]string)
	results, err := c.HGetAll(key)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(results); i += 2 {
		propMap[results[i]] = results[i+1]
	}

	u := jobUnmarshaller{}
	job := Job{
		ID:             u.atoi(propMap["id"]),
		State:          JobState(u.atoi(propMap["state"])),
		DelayedUntil:   u.atot(propMap["delayed_until"]),
		CreationTime:   u.atot(propMap["creation_time"]),
		CompletionTime: u.atot(propMap["completion_time"]),
		Priority:       u.atoi(propMap["priority"]),
		Payload:        u.parseJSON(propMap["payload"]),
		rawPayload:     propMap["payload"],
		NumAttempts:    u.atoi(propMap["num_attempts"]),
	}

	if u.Err != nil {
		panic(fmt.Sprintf("error during unmarshalling: %s", u.Err))
	}

	return &job, nil
}
