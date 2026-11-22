package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
)

type runtimeBuild struct{ actual, expected string }

func (b runtimeBuild) matches() bool { return b.expected == "" || b.actual == b.expected }
func loadRuntimeBuild(expected string) (runtimeBuild, error) {
	b := runtimeBuild{expected: expected}
	if expected != "" && !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(expected) {
		return b, errors.New("NEWNOA_EXPECTED_OWNER_SHA256 must be a lowercase SHA-256")
	}
	executable, err := os.Executable()
	if runtime.GOOS == "linux" {
		executable = "/proc/self/exe"
		err = nil
	}
	if err != nil {
		return b, err
	}
	file, err := os.Open(executable)
	if err != nil {
		return b, err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, file); err != nil {
		return b, err
	}
	b.actual = hex.EncodeToString(digest.Sum(nil))
	return b, nil
}
func (o *owner) runtimeIdentity(w http.ResponseWriter, r *http.Request) {
	if o.token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+o.token)) != 1 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"instance": o.id, "artifactSha256": o.build.actual, "buildPinned": o.build.expected != "", "buildMatches": o.build.matches()})
}
