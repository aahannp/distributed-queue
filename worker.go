package main

import (
	"context"
	"fmt"
	"time"
)

func execute[T isJob](fn func(T, context.Context, int) error, item T, ctx context.Context, workerId int, jm *JobManager) (res Result) {
	set := func(s JobState) {
		jm.mu.Lock()
		jm.setStateLocked(item.GetId(), s)
		jm.mu.Unlock()
	}
	defer func() {
		if r := recover(); r != nil {
			set(StateError)
			res = Result{ErrJobPanic, item.GetId()}
		}
	}()
	set(StateInProgress)
	err := fn(item, ctx, workerId)
	if err != nil {
		set(StateError)
		// add retry mech here and also the retry state
	} else {
		set(StateCompleted)
	}
	return Result{err, item.GetId()}
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
