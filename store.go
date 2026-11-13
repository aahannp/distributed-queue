package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var recordData = "data/jr.json"
var data = "data/jobs.json"

func saveAtomic(path string, recs map[int]JobRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
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

func openJSON[T any](path string) (T, error) {
	var result T
	data, err := os.ReadFile(path)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(data, &result)
	return result, err
}

func load(jm *JobManager) error {
	recs, err := openJSON[map[int]JobRecord](recordData)
	if errors.Is(err, os.ErrNotExist) {
		return nil // first run
	}
	if err != nil {
		return err
	}
	if jm.records == nil {
		jm.records = make(map[int]JobRecord, len(recs))
	}
	for _, value := range recs {
		jm.records[value.ID] = value
	}
	return nil
}

// caller must hold jm.mu, so the snapshot is consistent
func (jm *JobManager) persistLocked() {
	if err := saveAtomic(recordData, jm.records); err != nil {
		fmt.Println("save failed:", err)
	}
}
