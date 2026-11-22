package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func browserCleanupFixture(t *testing.T) (*browserAccount, string) {
	t.Helper()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		if r.Header.Get("Authorization") == "Bearer second" {
			id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	t.Cleanup(api.Close)
	c := newCore(t.TempDir(), api.URL)
	t.Cleanup(c.shutdown)
	a := &browserAccount{core: c}
	if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "first", api.URL); err != nil {
		t.Fatal(err)
	}
	return a, api.URL
}

func TestBrowserSwitchCancelledDrainStaysLockedAndCanRetry(t *testing.T) {
	a, control := browserCleanupFixture(t)
	ctx, release, err := a.begin("first", control)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- a.authenticate(httptest.NewRequest("POST", "/login", nil).WithContext(requestCtx), "second", control)
	}()
	<-ctx.Done()
	cancel()
	select {
	case err = <-result:
		if err == nil {
			t.Fatal("switch succeeded while old request remained")
		}
	case <-time.After(300 * time.Millisecond):
		release()
		<-result
		t.Fatal("cancelled switch kept switchMu while waiting for old request")
	}
	if a.active || !a.cleanupPending || a.core.proxySession != "" {
		t.Fatalf("unsafe cleanup state active=%v pending=%v session=%q", a.active, a.cleanupPending, a.core.proxySession)
	}
	if _, done, err := a.begin("first", control); err == nil {
		done()
		t.Fatal("old account entered while cleanup pending")
	}
	release()
	if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control); err != nil {
		t.Fatal(err)
	}
	if !a.active || a.cleanupPending || a.ownerID != "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee" || a.core.proxySession != "second" {
		t.Fatal("cleanup retry did not activate only new account")
	}
}

func TestBrowserSwitchCleanupAndPersistenceFailuresDoNotRestoreActiveOwner(t *testing.T) {
	for _, fault := range []string{"runtime", "persist"} {
		t.Run(fault, func(t *testing.T) {
			a, control := browserCleanupFixture(t)
			if fault == "runtime" {
				a.core.startupReady = true
				a.core.gateway = "file:///invalid"
			} else {
				if err := os.Remove(a.statePath()); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(a.statePath(), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control); err == nil {
				t.Fatal("fault did not fail switch")
			}
			if a.active || !a.cleanupPending || a.core.proxySession != "" {
				t.Fatalf("failed cleanup restored active owner: active=%v pending=%v session=%q", a.active, a.cleanupPending, a.core.proxySession)
			}
			if fault == "runtime" {
				a.core.startupReady = false
			} else if err := os.Remove(a.statePath()); err != nil {
				t.Fatal(err)
			}
			if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "first", control); err != nil {
				t.Fatal(err)
			}
			if !a.active || a.cleanupPending || a.core.proxySession != "first" {
				t.Fatal("same-account retry bypassed or failed cleanup")
			}
		})
	}
}

func TestBrowserCleanupDrainHasDefaultDeadline(t *testing.T) {
	for _, operation := range []string{"switch", "expire", "logout"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			a, control := browserCleanupFixture(t)
			_, release, err := a.begin("first", control)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			result := make(chan error, 1)
			go func() {
				switch operation {
				case "switch":
					result <- a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control)
				case "expire":
					result <- a.expire("first")
				case "logout":
					result <- a.logout("first", control)
				}
			}()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("unfinished old request accepted")
				}
			case <-time.After(6 * time.Second):
				release()
				<-result
				t.Fatal("cleanup exceeded bounded drain deadline")
			}
			if a.active || !a.cleanupPending || a.core.proxySession != "" {
				t.Fatal("timed-out cleanup did not fence account")
			}
			release()
			if operation == "logout" {
				if err := a.logout("first", control); err != nil {
					t.Fatal("logout retry", err)
				}
				if _, done, err := a.begin("first", control); err == nil {
					done()
					t.Fatal("logged-out owner reclaimed core without login")
				}
			} else if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control); err != nil {
				t.Fatal("switch retry", err)
			}
		})
	}
}

func TestBrowserSwitchRuntimeWaitHonorsCancellationAndReusesCleanup(t *testing.T) {
	a, control := browserCleanupFixture(t)
	a.core.svgWorkers.Add(1)
	workerDone := false
	defer func() {
		if !workerDone {
			a.core.svgWorkers.Done()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- a.authenticate(httptest.NewRequest("POST", "/login", nil).WithContext(ctx), "second", control)
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("switch accepted unfinished runtime worker")
		}
	case <-time.After(time.Second):
		a.core.svgWorkers.Done()
		workerDone = true
		<-result
		t.Fatal("runtime worker wait ignored cancellation")
	}
	if a.runtimeCleanup == nil || !a.cleanupPending || a.active {
		t.Fatal("unfinished runtime cleanup forgotten")
	}
	a.core.svgWorkers.Done()
	workerDone = true
	if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control); err != nil {
		t.Fatal(err)
	}
	if a.runtimeCleanup != nil || !a.active || a.cleanupPending {
		t.Fatal("runtime retry did not finish existing cleanup")
	}
}

func TestBrowserSwitchRetriesRuntimeFailureCompletedAfterCancellation(t *testing.T) {
	a, control := browserCleanupFixture(t)
	a.core.svgWorkers.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := a.authenticate(httptest.NewRequest("POST", "/login", nil).WithContext(ctx), "second", control); err == nil {
		a.core.svgWorkers.Done()
		t.Fatal("unfinished worker accepted")
	}
	a.core.startupReady = true
	a.core.gateway = "file:///invalid"
	a.core.svgWorkers.Done()
	previousErr := <-a.runtimeCleanup
	if previousErr == nil {
		t.Fatal("runtime fault not exercised")
	}
	a.runtimeCleanup <- previousErr
	a.core.startupReady = false
	if err := a.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control); err != nil {
		t.Fatal("repaired cleanup retried stale failure instead of stopping runtime", err)
	}
}
