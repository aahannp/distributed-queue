package main

import (
	"errors"
	"time"
)

var ErrInvalidJob = errors.New("this job is invalid")
var ErrServerBusy = errors.New("this server is busy")
var ErrJobPanic = errors.New("Job panic")
var MAX_TIME_PER_JOB = 500 * time.Millisecond
