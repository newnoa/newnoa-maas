package main

import (
	"strings"
	"testing"
	"time"
)

func TestPyroscopeConfigUsesSafeDefaultsAndBoundedTags(t *testing.T) {
	t.Setenv("PYROSCOPE_APPLICATION_NAME", "")
	t.Setenv("PYROSCOPE_SERVER_ADDRESS", "")
	t.Setenv("PYROSCOPE_UPLOAD_RATE", "")
	t.Setenv("NEWNOA_OTEL_ENVIRONMENT", "test")
	cfg, err := pyroscopeConfig("newnoa-owner")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ApplicationName != "newnoa-owner" || cfg.ServerAddress != "http://127.0.0.1:4040" || cfg.UploadRate != 10*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Tags["service_namespace"] != "newnoa" || cfg.Tags["deployment_environment"] != "test" {
		t.Fatalf("unexpected tags: %+v", cfg.Tags)
	}
	for _, profileType := range []string{"cpu", "alloc_objects", "inuse_space", "goroutines", "mutex_count", "block_duration"} {
		found := false
		for _, configured := range cfg.ProfileTypes {
			if string(configured) == profileType {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("profile type %q missing from %+v", profileType, cfg.ProfileTypes)
		}
	}
}

func TestPyroscopeConfigRejectsUnsafeEndpointAndApplication(t *testing.T) {
	t.Setenv("PYROSCOPE_SERVER_ADDRESS", "http://user:pass@127.0.0.1:4040?token=secret")
	if _, err := pyroscopeConfig("newnoa-owner"); err == nil {
		t.Fatal("expected endpoint validation error")
	}
	t.Setenv("PYROSCOPE_SERVER_ADDRESS", "http://127.0.0.1:4040")
	t.Setenv("PYROSCOPE_APPLICATION_NAME", "bad name")
	if _, err := pyroscopeConfig("newnoa-owner"); err == nil || !strings.Contains(err.Error(), "APPLICATION_NAME") {
		t.Fatalf("expected application validation error, got %v", err)
	}
}

func TestPyroscopeDisabledByDefault(t *testing.T) {
	t.Setenv("PYROSCOPE_ENABLED", "")
	startPyroscope("newnoa-owner")()
}
