package main

import (
	"errors"
	"time"
)

const (
	StatePending JobState = iota
	StateCompleted
	StateInProgress
	StateError
	StateRetrying
)

var jobStatus = map[JobState]string{
	StatePending:    "pending",
	StateCompleted:  "completed",
	StateInProgress: "InProgress",
	StateError:      "error",
	StateRetrying:   "retrying",
}
var maxRetry = 3

// moved the constants etc in the new file called constants.go in the same package
var ErrInvalidJob = errors.New("this job is invalid")
var ErrServerBusy = errors.New("this server is busy")
var ErrJobPanic = errors.New("Job panic")
var MAX_TIME_PER_JOB = 500 * time.Millisecond
