# koda-go #

A managed, hybrid priority/delayed job queue. Built on redis.

[![Build Status](https://travis-ci.org/cjlucas/koda-go.svg?branch=master)](https://travis-ci.org/cjlucas/koda-go)

## Getting Started ##

```go
// Add a job to the priority queue. Lowest priority is 0, highest priority is 100.
koda.Submit("send-newsletter", 100, map[string][]string{
    "users": []string{"bob@google.com"},
})

// Alternatively, a job can be put on the delayed queue.
koda.SubmitDelayed("send-newsletter", 5*time.Minute, map[string][]string{
    "users": []string{"mary@google.com"},
})

// Register a HandlerFunc and specify the # of workers to work the queue
koda.Register("send-newsletter", 10, func(job *koda.Job) error {
    // Unmarshal the payload specified by Submit/SubmitDelayed
    var payload map[string][]string
    if err := job.UnmarshalPayload(&payload); err != nil {
        return err
    }

    for _, user := range payload["users"] {
        fmt.Println("Sending newsletter to", user)
        // TODO: actually send the email to the user
    }

    return nil
})

koda.WorkForever()
```
