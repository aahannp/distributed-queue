package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

func main() {
	st := time.Now()
	jm := NewJobManager(2)
	err := load(jm)
	if err != nil {
		fmt.Println("load failed:", err)
		os.Exit(1)
	}
	jobs, err := openJSON[map[int]Job](data)
	if err != nil {
		fmt.Println("load failed:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	for key, rec := range jm.records {
		fmt.Printf("job %v returned %v after %d retries. \n", key, rec.State, rec.Attempts)
	}
	fmt.Println("main is done")
}
