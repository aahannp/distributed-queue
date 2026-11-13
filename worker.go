package main

import (
	"context"
	"fmt"
	"time"
)

func execute[T isJob](fn func(T, context.Context, int) error, item T, ctx context.Context, workerId int, jm *JobManager) (res Result) {
	defer func() {
		if r := recover(); r != nil {
			res = Result{ErrJobPanic, item.GetId()}
		}
	}()
	jm.mu.Lock()
	jm.setStateLocked(item.GetId(), StateInProgress)
	jm.mu.Unlock()

	err := fn(item, ctx, workerId)
	return Result{err, item.GetId()} // the worker decides what this means
}

func DoWork(job Job, ctx context.Context, workerId int) error {
	if job.Payload == "boom" {
		panic(fmt.Sprintf("the job %v is risky", job.Id))
	}
	if len(job.Payload) > 10 {
		return ErrInvalidJob
	}
	wait := 200 * time.Millisecond
	if job.Payload == "hang" {
		wait = 1000 * time.Millisecond
	}
	select {
	case <-time.After(wait):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
