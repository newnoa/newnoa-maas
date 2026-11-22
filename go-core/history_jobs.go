package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type historyPersistedJob struct {
	Job       historyJob `json:"job"`
	Owner     string     `json:"owner"`
	Kind      string     `json:"kind"`
	Scope     string     `json:"scope"`
	Directory string     `json:"directory"`
}

func (m *historyManager) persistJob(job *historyJob) error {
	raw, err := json.Marshal(historyPersistedJob{Job: *job, Owner: job.owner, Kind: job.kind, Scope: job.scope, Directory: m.config.ExportDirectory})
	if err != nil {
		return err
	}
	name := "history-job.json"
	if job.kind == "export" {
		name = "history-export-job.json"
	}
	if job.kind == "sync" {
		name = "history-sync-" + job.scope + ".json"
	}
	return atomicFile(m.privatePath(name), raw)
}
func (m *historyManager) loadJob() error {
	for _, name := range []string{"history-job.json", "history-export-job.json", "history-sync-own.json", "history-sync-others.json"} {
		if err := m.loadJobFile(m.privatePath(name)); err != nil {
			return err
		}
	}
	return nil
}
func (m *historyManager) loadJobFile(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe history job file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var saved historyPersistedJob
	if json.Unmarshal(raw, &saved) != nil {
		return errors.New("invalid history job file")
	}
	if saved.Job.JobID == "" {
		return nil
	}
	id := saved.Job.JobID
	if len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
		return errors.New("invalid history job id")
	}
	job := saved.Job
	if m.owner != "" && saved.Owner != m.owner {
		return errors.New("history job account mismatch")
	}
	job.owner = saved.Owner
	job.kind = saved.Kind
	job.scope = saved.Scope
	job.cancel = func() {}
	if job.State == "running" {
		job.State = "failed"
		job.Error = "history job interrupted; retry"
		job.Percent = min(job.Percent, 99)
		if job.kind == "export" && filepath.IsAbs(saved.Directory) {
			matches, _ := filepath.Glob(filepath.Join(saved.Directory, ".newnoa-export-"+id+"-*"))
			for _, name := range matches {
				if err = os.RemoveAll(name); err != nil {
					return err
				}
			}
		}
	}
	m.mu.Lock()
	m.jobs[id] = &job
	if job.kind == "export" {
		m.latestExport = id
	}
	if job.kind == "migration" {
		m.migration = id
	}
	if job.kind == "sync" {
		m.syncJobs[job.owner+"/"+job.scope] = id
	}
	m.mu.Unlock()
	return nil
}
