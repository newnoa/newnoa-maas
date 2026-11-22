package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlResponseRequiresOneCompleteBoundedJSONValue(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", `{"port":20000}`, true},
		{"whitespace", " \n" + `{"port":20000}` + "\n", true},
		{"truncated", `{"port":20000`, false},
		{"second-value", `{"port":20000}{"port":29999}`, false},
		{"trailing-garbage", `{"port":20000}garbage`, false},
		{"oversized", `{"port":20000}` + strings.Repeat(" ", maxBody), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(tc.body)) }))
			defer control.Close()
			o := newOwner("fixture", control.URL, "internal-fixture")
			var route struct {
				Port int `json:"port"`
			}
			err := o.call(context.Background(), "/route", map[string]any{}, &route)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v port=%d", tc.valid, err, route.Port)
			}
			if !tc.valid {
				request := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/responses", strings.NewReader(`{}`))
				request.Header.Set("Authorization", "Bearer internal-fixture")
				response := httptest.NewRecorder()
				o.ServeHTTP(response, request)
				if response.Code != 409 {
					t.Fatalf("invalid control route accepted: %d", response.Code)
				}
				request = httptest.NewRequest("POST", "/plugin", strings.NewReader(`{"op":"Login"}`))
				request.RemoteAddr = "127.0.0.1:12345"
				response = httptest.NewRecorder()
				o.ServeHTTP(response, request)
				if !strings.Contains(response.Body.String(), `"reject":true`) {
					t.Fatal("invalid control response allowed FRP login")
				}
			}
		})
	}
}
