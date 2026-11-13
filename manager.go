package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type JobManager struct {
	ch          chan Job
	wg          sync.WaitGroup
	mu          sync.Mutex
	records     map[int]JobRecord
	closed      bool
	deadletter  []int
	stopChannel chan struct{}
	outStanding int
}

func NewJobManager(size int) *JobManager {
	return &JobManager{
		ch:          make(chan Job, size),    // the way the main goroutine talks to the children
		records:     make(map[int]JobRecord), // the map of the job record
		closed:      false,                   // the status of the queue
		stopChannel: make(chan struct{}),     // this is the channel -> the way the parent goroutine talks the children to stop

	}
}
func (jm *JobManager) Close() {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.closed = true
	close(jm.ch)
}

func (jm *JobManager) Start(ctx context.Context, num int) {
	for i := 0; i < num; i++ {
		jm.wg.Add(1)
		go func() {
			defer jm.wg.Done()
			for {
				select {
				case job, ok := <-jm.ch:

					if !ok {
						return
					}
					jobCtx, jobCancel := context.WithTimeout(ctx, MAX_TIME_PER_JOB)
					res := execute(DoWork, job, jobCtx, i, jm)
					jobCancel()
					switch {
					case res.Err == nil, errors.Is(res.Err, ErrInvalidJob), errors.Is(res.Err, ErrJobPanic):
						jm.finishLocked() // final

					case errors.Is(res.Err, ErrServerBusy), errors.Is(res.Err, context.DeadlineExceeded):
						ok = jm.requeueJob(job, "server busy")
						if !ok {
							jm.finishLocked()
						}

					default:
						jm.requeueJob(job, "failed")
					}

				case <-ctx.Done():
					s := fmt.Sprintf("worker cancelled exiting :  %v", ctx.Err())
					fmt.Println(s)

					return

				}
			}
		}()
	}

}

// record the job and put in the chan
func (jm *JobManager) SubmitJob(job Job) {
	jm.mu.Lock()
	jm.records[job.Id] = JobRecord{ID: job.Id, Payload: job.Payload, State: StatePending}
	jm.persistLocked()
	jm.mu.Unlock()
	jm.ch <- job
}

// caller must hold jm.mu
func (jm *JobManager) setStateLocked(id int, s JobState) {
	rec := jm.records[id]
	rec.State = s
	jm.records[id] = rec
	jm.persistLocked()
}

func (jm *JobManager) Wait() {
	jm.wg.Wait()
}
func (jm *JobManager) finishLocked() {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.outStanding--
	// when all the workers are done
	if jm.outStanding == 0 {
		close(jm.stopChannel)
	}
}
