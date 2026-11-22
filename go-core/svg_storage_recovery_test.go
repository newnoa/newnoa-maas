package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSVGStoreRejectsUnexpectedDiskEntries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		makeEntry func(*testing.T, string)
	}{
		{"directory", func(t *testing.T, dir string) {
			if err := os.Mkdir(filepath.Join(dir, "unexpected"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{"invalid filename", func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, "not-a-job.json"), []byte(`{}`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", func(t *testing.T, dir string) {
			if err := os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, testModelID+".json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"invalid JSON", func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, testModelID+".json"), []byte(`{`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong identity", func(t *testing.T, dir string) {
			raw, _ := json.Marshal(svgTest{ID: "11111111-1111-4111-8111-111111111111", QuestionID: 1, Status: "succeeded"})
			if err := os.WriteFile(filepath.Join(dir, testModelID+".json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"oversized", func(t *testing.T, dir string) {
			f, err := os.Create(filepath.Join(dir, testModelID+".json"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err = f.Truncate(8*svgContentLimit + 1); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &core{home: t.TempDir(), control: "http://127.0.0.1:1"}
			scope := hash([]byte(c.control + "\naccount"))
			dir := filepath.Join(c.home, ".newnoa", "svg-tests", scope)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			tc.makeEntry(t, dir)
			if _, err := c.svgStore("account"); err == nil {
				t.Fatal("unsafe or corrupt stored job accepted")
			}
			if len(c.svgStores) != 0 {
				t.Fatal("failed load cached partial account state")
			}
		})
	}
}

func TestSVGStoreRecoversInterruptedJobsWithoutReplaying(t *testing.T) {
	c := &core{home: t.TempDir(), control: "http://127.0.0.1:1", publicStateless: true}
	store, err := c.svgStore("account")
	if err != nil {
		t.Fatal(err)
	}
	job := &svgTest{ID: testModelID, QuestionID: 1, Status: "running", Content: "<svg>partial</svg>"}
	if err = store.save(job); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(store.directory, ".newnoa-recovery"), []byte("ignored atomic marker"), 0600); err != nil {
		t.Fatal(err)
	}
	c.svgStores = nil
	loaded, err := c.svgStore("account")
	if err != nil {
		t.Fatal(err)
	}
	got := loaded.jobs[job.ID]
	if got.Status != "failed" || got.CompletedAt == "" || !strings.Contains(got.Error, "interrupted") || got.Content != "" || !got.contentOnDisk {
		t.Fatalf("interrupted job recovery=%+v", got)
	}
	detail, err := loaded.detail(got)
	if err != nil || detail["content"] != job.Content {
		t.Fatalf("partial result not recoverable: %v %v", detail, err)
	}
	raw, err := os.ReadFile(filepath.Join(loaded.directory, job.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted svgTest
	if json.Unmarshal(raw, &persisted) != nil || persisted.Status != "failed" {
		t.Fatal("recovered state was not durably saved")
	}
}

func TestSVGStoredDetailRejectsChangedIdentityAndMissingArtifact(t *testing.T) {
	store := &svgTestStore{directory: t.TempDir()}
	job := &svgTest{ID: testModelID, QuestionID: 1, Status: "succeeded", contentOnDisk: true}
	file := filepath.Join(store.directory, job.ID+".json")
	if _, err := store.detail(job); err == nil {
		t.Fatal("missing artifact accepted")
	}
	for _, body := range []string{`{`, `{"id":"another-job","content":"<svg/>"}`, `{"id":"` + job.ID + `","content":"` + strings.Repeat("x", svgContentLimit+1) + `"}`} {
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := store.detail(job); err == nil {
			t.Fatal("corrupted artifact accepted")
		}
	}
}

func TestSharedSVGStoreCannotEvictActiveAccountsToBypassBound(t *testing.T) {
	c := &core{home: t.TempDir(), control: "http://127.0.0.1:1", publicStateless: true, svgStores: map[string]*svgTestStore{}}
	for i := 0; i < 32; i++ {
		c.svgStores[strings.Repeat("x", i+1)] = &svgTestStore{workers: 1, jobs: map[string]*svgTest{}}
	}
	if _, err := c.svgStore("next-account"); err == nil {
		t.Fatal("shared active account bound exceeded")
	}
	if len(c.svgStores) != 32 {
		t.Fatal("active account state was evicted")
	}
	c = &core{home: t.TempDir(), control: "http://127.0.0.1:1"}
	if err := os.WriteFile(filepath.Join(c.home, ".newnoa"), []byte("blocked parent"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.svgStore("account"); err == nil {
		t.Fatal("non-directory task parent accepted")
	}
}
