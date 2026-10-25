package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// moved the constants etc in the new file called constants.go in the same package
var ErrInvalidJob = errors.New("this job is invalid")
var ErrServerBusy = errors.New("this server is busy")
var ErrJobPanic = errors.New("Job panic")
var MAX_TIME_PER_JOB = 500 * time.Millisecond

type Job struct {
	id      int
	payload string
}
type JobState int

type Result struct {
	Err   error
	JobId int
}

type isJob interface {
	GetId() int
}

type JobManager struct {
	ch     chan Job
	wg     sync.WaitGroup
	status map[int]string
	mu     sync.Mutex
	js     map[int]JobState
	jobs   map[int]Job
	timers map[int]*time.Timer
	closed bool
	// how many attempts have been made to retry a job // jobid-> no of retries
	attempts    map[int]int
	deadletter  []int
	stopChannel chan struct{}
	outStanding int
}

func NewJobManager(size int) *JobManager {
	return &JobManager{
		ch:          make(chan Job, size),
		status:      make(map[int]string),
		js:          make(map[int]JobState),
		jobs:        make(map[int]Job),
		timers:      make(map[int]*time.Timer),
		closed:      false,
		attempts:    make(map[int]int),
		stopChannel: make(chan struct{}),
	}
}

func (j Job) GetId() int { return j.id }

func (js JobState) String() string { return jobStatus[js] }

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

func (jm *JobManager) Close() {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.closed = true
	close(jm.ch)
}

// this would run on the main goroutine ? and wil be called by some other goroutione? crazy?
func (jm *JobManager) requeueJob(job Job, reason string) bool {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	delay := time.Millisecond * 100 << (jm.attempts[job.id])
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
		if tjob == job.id {
			indeadletter = true
			break
		}
	}

	if indeadletter {
		fmt.Printf("This job was in deadletter no requeue for this job %v \n", job.id)
		return false
	}

	// check if the job is already in deadletter we wont be requeueing it
	if jm.attempts[job.id] >= maxRetry {
		jm.js[job.id] = StateError
		jm.deadletter = append(jm.deadletter, job.id)
		fmt.Printf("job %d %s, max retries (%d) reached: dead-lettered\n", job.id, reason, maxRetry)
		return false
	}

	if jm.closed {
		fmt.Printf("job %d %s, but queue is closed: not requeued\n", job.id, reason)
		return false
	}
	select {
	case jm.ch <- job:
		jm.attempts[job.id]++
		jm.js[job.id] = StatePending
		fmt.Printf("job %d %s, requeued (retry %d/%d)\n", job.id, reason, jm.attempts[job.id], maxRetry)
		return true
	default:
		fmt.Printf("job %d %s, queue full: not requeued\n", job.id, reason)
		return false
	}

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

func execute[T isJob](fn func(T, context.Context, int, map[int]string, *sync.Mutex) error, item T, ctx context.Context, workerId int, jm *JobManager) (res Result) {
	mu := &jm.mu
	js := jm.js
	status := jm.status
	defer func() {
		if r := recover(); r != nil {
			mu.Lock()
			js[item.GetId()] = StateError
			mu.Unlock()
			res = Result{ErrJobPanic, item.GetId()}
		}
	}()
	mu.Lock()
	js[item.GetId()] = StateInProgress
	mu.Unlock()
	err := fn(item, ctx, workerId, status, mu)
	if err != nil {
		mu.Lock()
		js[item.GetId()] = StateError
		mu.Unlock()
		// add retry mech here and also the retry state
	} else {
		mu.Lock()
		js[item.GetId()] = StateCompleted
		mu.Unlock()
	}
	return Result{err, item.GetId()}
}

func DoWork(job Job, ctx context.Context, workerId int, status map[int]string, mu *sync.Mutex) error {
	if job.payload == "boom" {
		mu.Lock()
		status[job.id] = "risky job"
		mu.Unlock()
		s := fmt.Sprintf("the job %v is risky", job.id)
		panic(s)
	}
	if len(job.payload) > 10 {
		mu.Lock()
		status[job.id] = "invalid job"
		mu.Unlock()
		return ErrInvalidJob
	}
	if job.payload == "hang" {
		select {
		case <-time.After(1000 * time.Millisecond): // pretend to work for 1s
			mu.Lock()
			status[job.id] = "ok"
			mu.Unlock()
			return nil
		case <-ctx.Done(): // cancelled mid-job → stop now
			reason := "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				reason = "timed out"
			}
			mu.Lock()
			status[job.id] = reason
			mu.Unlock()
			return ctx.Err()
		}
	}
	// now you can pretend working
	select {
	case <-time.After(200 * time.Millisecond): // pretend to work for 200ms
		mu.Lock()
		status[job.id] = "ok"
		mu.Unlock()
	case <-ctx.Done(): // cancelled mid-job → stop now
		return ctx.Err()
	}
	return nil
}

func (jm *JobManager) SubmitJob(job Job) {
	jm.mu.Lock()
	jm.js[job.id] = StatePending
	jm.mu.Unlock()
	jm.ch <- job
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
func main() {
	st := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := []Job{
		{1, "2"},
		{2, "payload is bigger than 10"},
		{3, "boom"},
		{4, "busy"},
		{5, "hang"},
	}
	jm := NewJobManager(2)

	jm.Start(ctx, 5)
	jm.mu.Lock()
	jm.outStanding = len(jobs)
	jm.mu.Unlock()

	for _, job := range jobs {
		jm.SubmitJob(job)
	}
	<-jm.stopChannel
	end := time.Now()
	fmt.Println(end.Sub(st))
	jm.Close()
	jm.Wait()
	for key, val := range jm.status {
		fmt.Printf("job %v returned %v. \n", key, val)
	}
	fmt.Println("main is done everything is done thanks ")
}
