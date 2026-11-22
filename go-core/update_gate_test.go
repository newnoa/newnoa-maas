package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpdateGatePreservesActiveWorkAndBlocksNewWorkOnlyAfterPreparation(t *testing.T) {
	var gate runtimeUpdateGate
	if !gate.begin() {
		t.Fatal("normal work rejected")
	}
	if gate.prepare() == nil {
		t.Fatal("update must not interrupt active work")
	}
	gate.end()
	if err := gate.prepare(); err != nil {
		t.Fatal(err)
	}
	if gate.begin() {
		t.Fatal("prepared update accepted new work")
	}
	gate.cancel()
	if !gate.begin() {
		t.Fatal("cancelled update did not restore admission")
	}
	gate.end()
}
func TestUpdateGuardDoesNotExecuteInferenceAfterUpdatePreparation(t *testing.T) {
	c := &core{}
	calls := 0
	handler := c.updateGuard(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	if err := c.updateGate.prepare(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("POST", "/v1/chat/completions", nil))
	if response.Code != 503 || calls != 0 {
		t.Fatalf("code=%d calls=%d", response.Code, calls)
	}
}
