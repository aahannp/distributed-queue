package main

import (
	"fmt"
	"time"
)

func (jm *JobManager) requeueJob(job Job, reason string) bool {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	delay := time.Millisecond * 100 << (jm.records[job.Id].Attempts)
	time.AfterFunc(delay, func() {
		if !jm.requeueJobCore(job, reason) {
			jm.finishLocked()
		}
	})
	return true
}

func (jm *JobManager) requeueJobCore(job Job, reason string) bool {

	jm.mu.Lock()
	defer jm.mu.Unlock()

	indeadletter := false
	for _, tjob := range jm.deadletter {
		if tjob == job.Id {
			indeadletter = true
			break
		}
	}

	if indeadletter {
		fmt.Printf("This job was in deadletter no requeue for this job %v \n", job.Id)
		return false
	}

	// check if the job is already in deadletter we wont be requeueing it
	if jm.records[job.Id].Attempts >= maxRetry {
		jm.setStateLocked(job.Id, StateError)
		jm.deadletter = append(jm.deadletter, job.Id)
		fmt.Printf("job %d %s, max retries (%d) reached: dead-lettered\n", job.Id, reason, maxRetry)
		return false
	}

	if jm.closed {
		fmt.Printf("job %d %s, but queue is closed: not requeued\n", job.Id, reason)
		return false
	}
	select {
	case jm.ch <- job:
		rec := jm.records[job.Id]
		rec.Attempts++
		rec.State = StatePending
		jm.records[job.Id] = rec
		jm.persistLocked()
		fmt.Printf("job %d %s, requeued (retry %d/%d)\n", job.Id, reason, rec.Attempts, maxRetry)
		return true
	default:
		fmt.Printf("job %d %s, queue full: not requeued\n", job.Id, reason)
		return false
	}

}
