package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBalanceRuleRequiresVerifiedOriginCodeAndUnexecutedResponse(t *testing.T) {
	rules, err := parseBalanceRules(`[{"origin":"https://primary.example","status":402,"code":"insufficient_balance"}]`)
	if err != nil {
		t.Fatal(err)
	}
	errorBody := []byte(`{"error":{"code":"insufficient_balance","message":"balance unavailable"}}`)
	for _, tc := range []struct {
		name, endpoint string
		status         int
		body           []byte
		want           bool
	}{
		{"exact", "https://primary.example/v1", 402, errorBody, true},
		{"other origin", "https://secondary.example/v1", 402, errorBody, false},
		{"ordinary rate limit", "https://primary.example/v1", 429, errorBody, false},
		{"text alone", "https://primary.example/v1", 402, []byte(`{"error":{"message":"insufficient_balance"}}`), false},
		{"has usage", "https://primary.example/v1", 402, []byte(`{"error":{"code":"insufficient_balance"},"usage":{"total_tokens":1}}`), false},
		{"has data", "https://primary.example/v1", 402, []byte(`{"error":{"code":"insufficient_balance"},"data":[]}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			match := rules.match(tc.endpoint, tc.status, http.Header{"Content-Type": {"application/json"}}, tc.body)
			if (match != "") != tc.want {
				t.Fatalf("match = %q, want eligible=%v", match, tc.want)
			}
		})
	}
}

func TestGatewayBalanceRejectionRetriesOnceBeforeWritingResponse(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		status                   int
		reportOK, standbyRejects bool
		wantRetry                int
	}{
		{"switch succeeds", 402, true, false, 1},
		{"both reject", 402, true, true, 1},
		{"original report rejected", 402, false, false, 0},
		{"unrelated rate limit", 429, true, false, 0},
		{"interrupted stream", 200, true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var standbyCalls atomic.Int32
			primary := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer primary-key" {
					t.Error("incorrect primary credential")
				}
				if tc.status == 200 {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(200)
					io.WriteString(w, `data: {"type":"response.output_text.delta","delta":"first fragment"}`+"\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				io.WriteString(w, `{"error":{"code":"insufficient_balance"}}`)
			}))
			defer primary.Close()
			standby := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				standbyCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer standby-key" {
					t.Error("incorrect standby credential")
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.standbyRejects {
					w.WriteHeader(402)
					io.WriteString(w, `{"error":{"code":"insufficient_balance"}}`)
					return
				}
				io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"standby result"}]}],"usage":{"input_tokens":12,"output_tokens":2,"input_tokens_details":{"cached_tokens":0}}}`)
			}))
			defer standby.Close()
			var failedFinalized, primaryReported atomic.Bool
			var dispatches atomic.Int32
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture","deploymentId":"deployment"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					var request map[string]any
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if request["idempotencyKey"] != "caller-idem" {
						t.Error("caller idempotency key changed")
					}
					i := dispatches.Add(1)
					if i == 1 {
						io.WriteString(w, `{"requestId":"first","permit":"permit-first","endpoint":"`+primary.URL+`","apiKey":"primary-key","pooledCredential":true}`)
						return
					}
					proof, _ := request["balanceRetry"].(map[string]any)
					if !failedFinalized.Load() || !primaryReported.Load() || proof["requestId"] != "first" || proof["permit"] != "permit-first" {
						t.Errorf("invalid standby attempt %+v", proof)
					}
					io.WriteString(w, `{"requestId":"second","permit":"permit-second","endpoint":"`+standby.URL+`","apiKey":"standby-key","pooledCredential":true}`)
				case strings.HasSuffix(r.URL.Path, "/first/finalize"):
					var report map[string]any
					_ = json.NewDecoder(r.Body).Decode(&report)
					failedFinalized.Store(report["status"] == "failed" && report["executionClosed"] == true)
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/first/balance-rejection"):
					if !failedFinalized.Load() {
						t.Error("reported before attempt closed")
					}
					if !tc.reportOK {
						w.WriteHeader(409)
						return
					}
					primaryReported.Store(true)
					io.WriteString(w, `{"recorded":true}`)
				case strings.HasSuffix(r.URL.Path, "/second/balance-rejection"):
					io.WriteString(w, `{"recorded":true}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			g.egress = primary.Client()
			g.balanceRules = balanceRules{
				{Origin: primary.URL, Status: 402, Code: "insufficient_balance"},
				{Origin: standby.URL, Status: 402, Code: "insufficient_balance"},
			}
			body := `{"model":"fixture","input":"fixture request"}`
			if tc.status == 200 {
				body = `{"model":"fixture","input":"fixture request","stream":true}`
			}
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer ses_caller")
			req.Header.Set("X-Api-Key", "nn_caller")
			req.Header.Set("Idempotency-Key", "caller-idem")
			response := httptest.NewRecorder()
			func() {
				defer func() {
					if recoverValue := recover(); recoverValue != nil && (tc.status != 200 || recoverValue != http.ErrAbortHandler) {
						panic(recoverValue)
					}
				}()
				g.ServeHTTP(response, req)
			}()
			if int(standbyCalls.Load()) != tc.wantRetry || int(dispatches.Load()) != 1+tc.wantRetry {
				t.Fatalf("dispatches=%d standby=%d", dispatches.Load(), standbyCalls.Load())
			}
			if tc.wantRetry == 1 && !tc.standbyRejects {
				if response.Code != 200 || !strings.Contains(response.Body.String(), "standby result") || response.Header().Get("X-Request-Id") != "second" {
					t.Fatalf("unexpected standby response %d %s", response.Code, response.Body.String())
				}
			} else if tc.status == 200 {
				if response.Code != 200 || !strings.Contains(response.Body.String(), "first fragment") || primaryReported.Load() {
					t.Fatalf("interrupted stream changed: %d %s", response.Code, response.Body.String())
				}
			} else if response.Code != 502 {
				t.Fatalf("expected failure, got %d", response.Code)
			}
			if strings.Contains(response.Body.String(), "insufficient_balance") {
				t.Fatal("supplier error leaked")
			}
		})
	}
}

func TestSupplierBalanceRejectionUsesExactTopLevelCode(t *testing.T) {
	rules, err := parseBalanceRules(`[{"origin":"https://supplier.example.test","status":403,"code":"INSUFFICIENT_BALANCE","codeField":"code"}]`)
	if err != nil {
		t.Fatal(err)
	}
	const endpoint = "https://supplier.example.test/v1/responses"
	const observed = `{"code":"INSUFFICIENT_BALANCE","message":"Insufficient account balance"}`
	headers := http.Header{"Content-Type": {"application/json; charset=utf-8"}}
	if code := rules.match(endpoint, 403, headers, []byte(observed)); code != "INSUFFICIENT_BALANCE" {
		t.Fatalf("configured supplier refusal matched %q", code)
	}
	for _, test := range []struct {
		status int
		body   string
	}{
		{429, observed},
		{403, `{"code":"FORBIDDEN","message":"Insufficient account balance"}`},
		{403, `{"error":{"code":"INSUFFICIENT_BALANCE"}}`},
		{403, `{"code":"INSUFFICIENT_BALANCE","error":{"code":"FORBIDDEN"}}`},
		{403, `{"code":"INSUFFICIENT_BALANCE","usage":{"total_tokens":1}}`},
	} {
		if code := rules.match(endpoint, test.status, headers, []byte(test.body)); code != "" {
			t.Fatalf("ambiguous response %d matched %q", test.status, code)
		}
	}
	if code := rules.match("https://apiking.ai/v1/responses", 403, headers, []byte(observed)); code != "" {
		t.Fatalf("other supplier matched configured rule: %q", code)
	}
}
