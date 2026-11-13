package main

type JobState int

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

func (js JobState) String() string { return jobStatus[js] }
