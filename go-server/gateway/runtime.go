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
		return b, errors.New("NEWNOA_EXPECTED_GATEWAY_SHA256 must be a lowercase SHA-256")
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
func (g *gateway) runtimeIdentity(w http.ResponseWriter, r *http.Request) {
	if g.token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+g.token)) != 1 {
		fail(w, 401, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"instance": g.id, "artifactSha256": g.build.actual, "buildPinned": g.build.expected != "", "buildMatches": g.build.matches()})
}
