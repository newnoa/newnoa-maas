package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInferenceBodyReusesOnlyCurrentBytes(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader("original"))
	first, err := readInferenceBody(r)
	if err != nil {
		t.Fatal(err)
	}
	again, err := readInferenceBody(r)
	if err != nil || &first[0] != &again[0] {
		t.Fatal("snapshot was copied")
	}
	setInferenceBody(r, []byte("rewritten"))
	raw, err := readInferenceBody(r)
	if err != nil || string(raw) != "rewritten" || r.GetBody != nil {
		t.Fatal("stale or replayable payload")
	}
	out, err := io.ReadAll(r.Body)
	if err != nil || string(out) != string(raw) {
		t.Fatal("body consumed before forwarding")
	}
}

func TestInferenceUploadAdmissionPrecedesRead(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader("body"))
	r.ContentLength = -1
	release := inferenceUploads.Reserve(r.RemoteAddr, 32<<20)
	if release == nil {
		t.Fatal("fixture reservation failed")
	}
	defer release()
	w := httptest.NewRecorder()
	if got := reserveInferenceUpload(w, r, r.RemoteAddr); got != nil || w.Code != http.StatusTooManyRequests {
		t.Fatal("busy upload was admitted")
	}
	original, _ := io.ReadAll(r.Body)
	if string(original) != "body" {
		t.Fatal("rejected upload consumed body")
	}
}
