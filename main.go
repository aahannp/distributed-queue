package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func main() {
	st := time.Now()
	recs, err := load()
	if err != nil {
		fmt.Println("load failed:", err)
		os.Exit(1)
	}
	fmt.Println(len(recs))
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
		_, err := json.Marshal(job)
		if err != nil {
			fmt.Println("Error marshalling:", err)
			return
		}
		jm.SubmitJob(job)
	}
	<-jm.stopChannel // give a cmd to stop the channel
	end := time.Now()
	fmt.Println(end.Sub(st))
	jm.Close() // closing the main goroutine - the sender - the channel is closed
	jm.Wait()
	for key, rec := range jm.records {
		fmt.Printf("job %v returned %v after %d retries. \n", key, rec.State, rec.Attempts)
	}
	// havent updated the job states after thw whole implementation
	fmt.Println("main is done everything is done thanks ")
}
