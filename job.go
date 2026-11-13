package main

import (
	"errors"
	"fmt"
)

type Job struct {
	Id      int    `json:"id"`
	Payload string `json:"payload,omitempty"`
}
type isJob interface {
	GetId() int
}

func (j Job) GetId() int { return j.Id }
func (r Result) String() string {
	switch {
	case r.Err == nil:
		return fmt.Sprintf("the job %d is ok", r.JobId)
	case errors.Is(r.Err, ErrInvalidJob):
		return fmt.Sprintf("this job %d is good %v", r.JobId, r.Err)
	default:
		return fmt.Sprintf("job %d: failed, retry later: %v", r.JobId, r.Err)
	}
}

type Result struct {
	Err   error
	JobId int
}
type JobRecord struct {
	ID       int      `json:"id"`
	Payload  string   `json:"payload"`
	State    JobState `json:"state"`
	Attempts int      `json:"attempts"`
}
