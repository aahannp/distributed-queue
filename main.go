package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// moved the constants etc in the new file called constants.go in the same package
var ErrInvalidJob = errors.New("this job is invalid")
var ErrServerBusy = errors.New("this server is busy")
var ErrJobPanic = errors.New("Job panic")
var MAX_TIME_PER_JOB = 500 * time.Millisecond

type Job struct {
	Id      int    `json:"id"`
	Payload string `json:"payload,omitempty"`
}
type JobState int
type JobRecord struct {
	ID       int      `json:"id"`
	Payload  string   `json:"payload"`
	State    JobState `json:"state"`
	Attempts int      `json:"attempts"`
}
type Result struct {
	Err   error
	JobId int
}

type isJob interface {
	GetId() int
}

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
		ch:          make(chan Job, size),
		records:     make(map[int]JobRecord),
		closed:      false,
		stopChannel: make(chan struct{}),
	}
}

func (j Job) GetId() int { return j.Id }

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
func saveAtomic(path string, recs map[int]JobRecord) error {
	data, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path) // either the old file or the new one, never half
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

// caller must hold jm.mu, so the snapshot is consistent
func (jm *JobManager) persistLocked() {
	if err := saveAtomic("jobs.json", jm.records); err != nil {
		fmt.Println("save failed:", err)
	}
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
		jsonData, err := json.Marshal(job)
		if err != nil {
			fmt.Println("Error marshalling:", err)
			return
		}

		// json.Marshal returns a byte slice, so we cast it to a string to print it
		fmt.Println(string(jsonData))
		jm.SubmitJob(job)
	}
	<-jm.stopChannel
	end := time.Now()
	fmt.Println(end.Sub(st))
	jm.Close()
	jm.Wait()
	for key, rec := range jm.records {
		fmt.Printf("job %v returned %v after %d retries. \n", key, rec.State, rec.Attempts)
	}
	fmt.Println("main is done everything is done thanks ")
}
