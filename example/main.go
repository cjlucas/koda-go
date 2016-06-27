package main

import (
	"errors"
	"fmt"

	"github.com/cjlucas/koda-go"
)

func main() {
	koda.Submit("q", 100, nil)
	koda.Register("q", 1, func(job koda.Job) error {
		fmt.Println("hello world")
		return errors.New("")
	})

	koda.WorkForever()
}
