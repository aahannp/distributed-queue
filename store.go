package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var datapath = "data/jobs.json"

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

func load() (map[int]JobRecord, error) {
	path := datapath
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // first run
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var recs map[int]JobRecord
	if err := json.NewDecoder(file).Decode(&recs); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return recs, nil
}

// caller must hold jm.mu, so the snapshot is consistent
func (jm *JobManager) persistLocked() {
	if err := saveAtomic(datapath, jm.records); err != nil {
		fmt.Println("save failed:", err)
	}
}
