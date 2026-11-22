package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCoreAPIKeyAndSessionReachAPIAuthorizationSeparately(t *testing.T) {
	var seen map[string]any
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&seen)
		if seen["secret"] != "application-key" || seen["session"] != "ses_client_login" {
			w.WriteHeader(401)
			return
		}
		w.Write([]byte(`[]`))
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	request := httptest.NewRequest("GET", "/v1/models", nil)
	request.Header.Set("X-Api-Key", "application-key")
	request.Header.Set("Authorization", "Bearer ses_client_login")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != 200 || seen["secret"] != "application-key" || seen["session"] != "ses_client_login" {
		t.Fatalf("dual credentials not authorized: status=%d", response.Code)
	}
	request.Header.Set("Authorization", "Bearer ses_expired")
	response = httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatalf("API rejection lost: status=%d", response.Code)
	}
	for _, credentials := range [][2]string{{"application-key", ""}, {"", "Bearer ses_client_login"}} {
		request.Header.Set("X-Api-Key", credentials[0])
		request.Header.Set("Authorization", credentials[1])
		seen = nil
		response = httptest.NewRecorder()
		g.ServeHTTP(response, request)
		if response.Code != 401 || seen != nil {
			t.Fatalf("incomplete credential pair accepted: status=%d", response.Code)
		}
	}
}
