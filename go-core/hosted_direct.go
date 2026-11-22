package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"time"
)

type hostedModel struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Model     string `json:"model"`
	ModelType string `json:"modelType"`
}

var hostedReportToken = regexp.MustCompile(`^hreport_[a-zA-Z0-9_-]{40,90}$`)

type hostedDispatch struct {
	RequestID           string          `json:"requestId"`
	AccountID           string          `json:"accountId"`
	KeyID               string          `json:"keyId"`
	DeploymentID        string          `json:"-"`
	ReportToken         string          `json:"reportToken"`
	Model               string          `json:"model"`
	ModelType           string          `json:"modelType"`
	Endpoint            string          `json:"endpoint"`
	APIKey              string          `json:"apiKey"`
	Revision            int64           `json:"revision"`
	ExecutionDeadlineAt json.RawMessage `json:"executionDeadlineAt,omitempty"`
}

// Hosted completion credentials can only finalize their original request, across logout/restart.
func hostedControl(ctx context.Context, control, path, authorization, key string, body, result any) error {
	if _, err := controlBase(control); err != nil {
		return errors.New("hosted control address invalid")
	}
	return hostedControlAt(ctx, control, path, authorization, key, body, result)
}

func (c *core) hostedRemote(ctx context.Context, path, authorization, key string, body, result any) error {
	if !c.publicStateless {
		return hostedControl(ctx, c.control, path, authorization, key, body, result)
	}
	if err := validatePublicControlOrigin(c.control); err != nil {
		return err
	}
	return hostedControlAt(ctx, c.control, path, authorization, key, body, result)
}

func hostedControlAt(ctx context.Context, control, path, authorization, key string, body, result any) error {
	method := http.MethodGet
	var raw []byte
	if body != nil {
		method = http.MethodPost
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	call, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(control, "/")+"/api/v1/client/hosted"+path, bytes.NewReader(raw))
	if err != nil {
		return errors.New("hosted control address invalid")
	}
	call.Header.Set("Authorization", authorization)
	if key != "" {
		call.Header.Set("X-Api-Key", key)
	}
	call.Header.Set("Content-Type", "application/json")
	client := connectorControlClient()
	client.Timeout = 10 * time.Second
	response, err := client.Do(call)
	if err != nil {
		return errors.New("hosted control unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &hostedControlHTTPError{status: response.StatusCode}
	}
	if result != nil {
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if readErr != nil || len(raw) > 1<<20 || json.Unmarshal(raw, result) != nil {
			return errors.New("hosted control response invalid")
		}
	}
	return nil
}
func (c *core) onlinePolicy() supplyPolicy {
	if c.onlineSupply != nil {
		return *c.onlineSupply
	}
	return supplyPolicy{edition: "online"}
}
func (c *core) hostedModels(r *http.Request) ([]hostedModel, error) {
	var models []hostedModel
	err := c.hostedRemote(r.Context(), "/models", r.Header.Get("Authorization"), r.Header.Get("X-Api-Key"), nil, &models)

	return models, err
}
func hostedRequestedModel(r *http.Request, raw []byte) string {
	media, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if media == "multipart/form-data" {
		reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
		for {
			part, err := reader.NextPart()
			if err != nil {
				return ""
			}
			if part.FormName() == "model" {
				value, _ := io.ReadAll(io.LimitReader(part, 201))
				part.Close()
				return string(value)
			}
			part.Close()
		}
	}
	var body struct {
		Model string `json:"model"`
	}
	json.Unmarshal(raw, &body)
	return body.Model
}
func (c *core) hostedInference(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") == "" && !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ses_") {
			r = r.Clone(r.Context())
			c.attachProxySession(r)
		}
		eligible := strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ses_") && r.Header.Get("X-Api-Key") != "" && r.URL.RawQuery == ""
		if !eligible {
			next.ServeHTTP(w, r)
			return
		}
		c.proxySessionMu.RLock()
		current, sessionCtx := c.proxySession, c.proxySessionContext
		c.proxySessionMu.RUnlock()
		if sessionCtx != nil && r.Header.Get("Authorization") != "Bearer "+current {
			historyRejectBeforeAuthorization(w, "Core account session changed", 401)
			return
		}
		if sessionCtx != nil {
			ctx, cancel := context.WithCancel(r.Context())
			stop := context.AfterFunc(sessionCtx, cancel)
			defer stop()
			defer cancel()
			r = r.WithContext(ctx)
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
			models, err := c.hostedModels(r)
			if err != nil {
				http.Error(w, "hosted model authorization unavailable", 502)
				return
			}
			if len(models) == 0 {
				next.ServeHTTP(w, r)
				return
			}
			result := httptest.NewRecorder()
			gatewayCtx, stop := context.WithTimeout(r.Context(), 2*time.Second)
			next.ServeHTTP(result, r.WithContext(gatewayCtx))
			stop()
			var listing struct {
				Data []map[string]any `json:"data"`
			}
			if result.Code != 200 || json.Unmarshal(result.Body.Bytes(), &listing) != nil {
				listing.Data = nil
			}
			seen := map[string]bool{}
			for _, row := range listing.Data {
				id, _ := row["id"].(string)
				seen[id] = true
			}
			for _, model := range models {
				if nodePattern.MatchString(model.ID) && !seen[model.ID] {
					listing.Data = append(listing.Data, map[string]any{"id": model.ID, "object": "model", "owned_by": "private", "name": model.Name, "model": model.Model, "modelType": model.ModelType})
					seen[model.ID] = true
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": listing.Data})
			return
		}
		if r.Method != http.MethodPost || !historyInferencePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		raw, err := readInferenceBody(r)
		if err != nil {
			http.Error(w, "request too large", 413)
			return
		}
		model := hostedRequestedModel(r, raw)
		if !nodePattern.MatchString(model) {
			next.ServeHTTP(w, r)
			return
		}
		models, err := c.hostedModels(r)
		if err != nil {
			historyRejectBeforeAuthorization(w, "hosted model authorization unavailable", 502)
			return
		}
		for _, target := range models {
			if target.ID == model {
				c.executeHosted(w, r, model, raw)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
func (h *chatHistory) queueHosted(account string, d hostedDispatch, report usageReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = h.db.Exec("INSERT INTO hosted_usage_outbox(request_id,account,model_id,revision,key_id,report_token,payload,next_attempt) VALUES(?,?,?,?,?,?,?,unixepoch()+600)", d.RequestID, account, d.DeploymentID, d.Revision, d.KeyID, d.ReportToken, string(raw))
	return err
}

func (c *core) beginHosted(h *chatHistory, account string, d hostedDispatch, report usageReport) error {
	c.hostedMu.Lock()
	defer c.hostedMu.Unlock()
	if err := h.queueHosted(account, d, report); err != nil {
		return err
	}
	if c.hostedActive == nil {
		c.hostedActive = make(map[string]bool)
	}
	c.hostedActive[d.RequestID] = true
	return nil
}

func (c *core) completeHosted(h *chatHistory, account string, d hostedDispatch, report usageReport) error {
	c.hostedMu.Lock()
	defer c.hostedMu.Unlock()
	// Execution has ended; failed persistence must leave the crash-safe unknown retryable.
	defer delete(c.hostedActive, d.RequestID)
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	// Recreate a missing row, but never overwrite another request's saved identity.
	result, err := h.db.Exec(`INSERT INTO hosted_usage_outbox(request_id,account,model_id,revision,key_id,report_token,payload,next_attempt)
	 VALUES(?,?,?,?,?,?,?,0) ON CONFLICT(request_id) DO UPDATE SET payload=excluded.payload,next_attempt=0
	 WHERE account=excluded.account AND model_id=excluded.model_id AND revision=excluded.revision
	 AND key_id=excluded.key_id AND report_token=excluded.report_token`, d.RequestID, account, d.DeploymentID, d.Revision, d.KeyID, d.ReportToken, string(raw))
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return errors.New("hosted completion was not persisted")
	}
	return nil
}
func (c *core) flushHosted(ctx context.Context, h *chatHistory) {
	if !c.hostedMu.TryLock() {
		return
	}
	defer c.hostedMu.Unlock()
	rows, err := h.db.QueryContext(ctx, "SELECT request_id,report_token,payload,attempts FROM hosted_usage_outbox WHERE next_attempt<=unixepoch() ORDER BY created_at,request_id LIMIT 20")
	if err != nil {
		return
	}
	type pending struct {
		id, token, raw string
		attempts       int
	}
	batch := []pending{}
	for rows.Next() {
		var p pending
		if rows.Scan(&p.id, &p.token, &p.raw, &p.attempts) != nil {
			break
		}
		batch = append(batch, p)
	}
	rows.Close()
	for _, p := range batch {
		if c.hostedActive[p.id] {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		var report usageReport
		if json.Unmarshal([]byte(p.raw), &report) != nil {
			continue
		}
		if len(report.PendingMeter) > 0 {
			d := hostedDispatch{RequestID: p.id, ReportToken: p.token}
			var snapshot metering.Snapshot
			deliveryCtx, stop := context.WithTimeout(ctx, 3*time.Second)
			deliveryErr := c.checkpointHosted(deliveryCtx, d, report.MeterSequence+1, report.PendingMeter)
			stop()
			permanentReject := errors.Is(deliveryErr, errHostedCheckpointRejected)
			if !permanentReject && (deliveryErr != nil || json.Unmarshal(report.PendingMeter, &snapshot) != nil) {
				h.db.Exec("UPDATE hosted_usage_outbox SET attempts=attempts+1,next_attempt=unixepoch()+?,last_error='hosted_checkpoint_pending' WHERE request_id=?", min(10<<min(p.attempts, 5), 300), p.id)
				continue
			}
			if permanentReject {
				// These bytes were withheld before ack. Close/finalize only the
				// accepted prefix; never charge rejected observations or loop forever.
				log.Printf("hosted_checkpoint_pending_rejected request=%s", p.id)
			} else {
				report.MeterSequence++
				report.ImageUsage, report.ImageMetadata = snapshot.ImageUsage, snapshot.ImageMetadata
				if usage := snapshot.Reported; usage != nil {
					report.Input, report.Output, report.Cached, report.Write = usage.InputTokens, usage.OutputTokens, usage.CachedInputTokens, usage.CacheWriteTokens
				}
			}
			report.PendingMeter = nil
			persisted, persistErr := json.Marshal(report)
			if persistErr != nil {
				continue
			}
			if _, persistErr = h.db.Exec("UPDATE hosted_usage_outbox SET payload=? WHERE request_id=?", string(persisted), p.id); persistErr != nil {
				continue
			}
		}
		payload := hostedUsagePayload(report)
		var ack struct {
			Finalized bool `json:"finalized"`
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = c.hostedRemote(callCtx, "/dispatches/"+p.id+"/finish", "Bearer "+p.token, "", payload, &ack)
		cancel()
		if err == nil && ack.Finalized {
			h.db.Exec("DELETE FROM hosted_usage_outbox WHERE request_id=?", p.id)
		} else {
			h.db.Exec("UPDATE hosted_usage_outbox SET attempts=attempts+1,next_attempt=unixepoch()+?,last_error='hosted_completion_pending' WHERE request_id=?", min(10<<min(p.attempts, 5), 300), p.id)
		}
	}
}
func (c *core) startHostedSync(h *chatHistory) {
	ctx, cancel := context.WithCancel(context.Background())
	c.hostedCancel = cancel
	c.hostedDone = make(chan struct{})
	maintenanceDone := make(chan struct{})
	maintenanceStarted := make(chan struct{})
	consumptionDone := make(chan struct{})
	h.consumptionWork = make(chan historyConsumptionWork, 32)
	go func() { defer close(consumptionDone); c.runConsumption(ctx, h) }()
	go func() {
		defer close(maintenanceDone)
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		h.manager.runMaintenance(ctx, ticker.C, maintenanceStarted)
	}()
	go func() {
		defer close(c.hostedDone)
		defer func() { <-maintenanceDone }()
		defer func() { <-consumptionDone }()
		select {
		case <-maintenanceStarted:
		case <-ctx.Done():
			return
		}
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		c.flushHosted(ctx, h)
		c.autoHistorySync(ctx, h)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.flushHosted(ctx, h)
				c.autoHistorySync(ctx, h)
			}
		}
	}()
}
func (c *core) executeHosted(w http.ResponseWriter, r *http.Request, model string, raw []byte) {
	if isImagePath(r.URL.Path) && validateImageRequest(r.URL.Path, raw) != nil {
		http.Error(w, "invalid image generation request", http.StatusBadRequest)
		return
	}
	timing := peernet.CallTiming{Started: time.Now()}
	ctx := r.Context()
	var h *chatHistory
	var err error
	if !c.publicStateless {
		h, err = c.historyStore()
		if err != nil {
			http.Error(w, "hosted usage storage unavailable", 503)
			return
		}
	}
	var account struct {
		ID string `json:"id"`
	}
	if !c.publicStateless && (historyRemote(ctx, c.control, "/me", r.Header.Get("Authorization"), nil, &account) != nil || !nodePattern.MatchString(account.ID)) {
		historyRejectBeforeAuthorization(w, "hosted account unavailable", 401)
		return
	}
	prefix := "POST " + r.URL.Path + "\n"
	if r.URL.Path == "/v1/audio/transcriptions" {
		prefix += r.Header.Get("Content-Type") + "\n"
	}
	beta := ""
	if r.URL.Path == "/v1/messages" {
		beta, err = messagesBeta(r.Header)
		if err != nil {
			historyRejectBeforeAuthorization(w, "invalid messages beta", 400)
			return
		}
		prefix += messagesBinding(beta)
	}
	attemptID, attemptErr := uuid.NewRandom()
	if attemptErr != nil {
		http.Error(w, "hosted admission unavailable; request was not replayed", http.StatusBadGateway)
		return
	}
	params := map[string]any{"attemptId": attemptID.String(), "model": model, "inferencePath": r.URL.Path, "bodyDigest": hash(append([]byte(prefix), raw...))}
	var metadataBody map[string]any
	json.Unmarshal(raw, &metadataBody)
	for key, value := range peernet.CallMetadata(r, metadataBody) {
		params[key] = value
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		params["idempotencyKey"] = key
	}
	var d hostedDispatch
	// Admission may already have entered execution even when its response is lost. Never fall back.
	if c.publicStateless {
		params["secret"] = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if session, _ := r.Context().Value(publicCallerSessionContext{}).(string); session != "" {
			params["session"] = session
		}
	}
	admission := newHostedAdmissionCaller(c, ctx, r.Header.Get("Authorization"), r.Header.Get("X-Api-Key"), params)
	err = admission.create(ctx, params, &d)
	if err != nil {
		if !explicitHostedRejection(err) && admission.abort(ctx) != nil {
			log.Printf("hosted_admission_abort_pending")
		}
		http.Error(w, "hosted admission outcome unknown; request was not replayed", 502)
		return
	}
	if c.publicStateless {
		account.ID = d.AccountID
	}
	if !nodePattern.MatchString(account.ID) || d.AccountID != account.ID || !nodePattern.MatchString(d.KeyID) || !nodePattern.MatchString(d.RequestID) || !hostedReportToken.MatchString(d.ReportToken) || d.Model == "" || len(d.Model) > 200 || d.Revision < 1 || strings.ContainsAny(d.APIKey, "\r\n") || len(d.APIKey) > 4096 {
		if admission.abort(ctx) != nil {
			log.Printf("hosted_admission_abort_pending")
		}
		http.Error(w, "hosted admission response invalid; request was not replayed", 502)
		return
	}
	d.DeploymentID = model
	report := usageReport{Status: "unknown"}
	var meter *hostedMeter
	defer func() {
		if diagnostic, ok := r.Context().Value(inferenceDiagnosticContext{}).(*inferenceDiagnostic); ok {
			diagnostic.outcome = report.Status
		}
	}()
	if !c.publicStateless {
		err = c.beginHosted(h, account.ID, d, report)
	}
	if err != nil {
		http.Error(w, "hosted request already admitted or storage unavailable; request was not replayed", 503)
		return
	}
	w.Header().Set("X-Newnoa-Request", d.RequestID)
	w.Header().Set("X-Request-Id", d.RequestID)
	w.Header().Set("X-Newnoa-Path", "core-hosted-direct")
	w.Header().Set("X-Newnoa-Model-Type", d.ModelType)
	w.Header().Set("X-Newnoa-Usage", "pending")
	defer func() {
		if meter != nil && len(meter.pending) > 0 {
			retryCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			if recoveryErr := meter.recoverCheckpoint(retryCtx, c, h, account.ID, d, &report); recoveryErr != nil {
				if errors.Is(recoveryErr, errHostedCheckpointRejected) {
					log.Printf("hosted_checkpoint_pending_rejected request=%s", d.RequestID)
				} else {
					log.Printf("hosted_checkpoint_recovery_pending request=%s", d.RequestID)
				}
			}
			stop()
			if len(meter.pending) > 0 {
				report.PendingMeter = append(json.RawMessage(nil), meter.pending...)
				if c.publicStateless {
					return
				} // API expiry settles its durable latest observation.
			}
		}
		metrics := map[string]any{}
		timing.Finish(metrics, report.Output, time.Now())
		report.DurationMs = metrics["durationMs"].(int64)
		if first, ok := metrics["firstTokenMs"].(int64); ok {
			report.FirstTokenMs = &first
		}
		if tps, ok := metrics["outputTps"].(float64); ok {
			report.OutputTps = &tps
		}
		if c.publicStateless {
			finishCtx, stop := context.WithTimeout(context.Background(), 12*time.Second)
			defer stop()
			var ack struct {
				Finalized bool `json:"finalized"`
			}
			if c.hostedRemote(finishCtx, "/dispatches/"+d.RequestID+"/finish", "Bearer "+d.ReportToken, "", hostedUsagePayload(report), &ack) != nil || !ack.Finalized {
				log.Printf("public_hosted_completion_pending request=%s", d.RequestID)
			}
		} else if saveErr := c.completeHosted(h, account.ID, d, report); saveErr == nil {
			finishCtx, stop := context.WithTimeout(context.Background(), 12*time.Second)
			defer stop()
			c.flushHosted(finishCtx, h)
		} else {
			log.Printf("hosted_completion_storage_failed request=%s", d.RequestID)
		}
	}()
	policy := c.onlinePolicy()
	ctx, cancel, deadlineErr := executionContext(ctx, d.ExecutionDeadlineAt, time.Now())
	if deadlineErr != nil {
		report.Status = "failed"
		http.Error(w, "hosted execution deadline invalid; request was not replayed", http.StatusBadGateway)
		return
	}
	defer cancel()
	base, err := policy.endpoint(d.Endpoint)
	if err != nil {
		report.Status = "failed"
		http.Error(w, "hosted upstream address invalid", 502)
		return
	}
	base.Path = modelServiceBase(base.Path)
	if !strings.HasSuffix(base.Path, "/v1") {
		base.Path += "/v1"
	}
	base.Path += strings.TrimPrefix(r.URL.Path, "/v1")
	base.RawPath = ""
	encoded := raw
	contentType := r.Header.Get("Content-Type")
	stream := false
	if r.URL.Path == "/v1/audio/transcriptions" {
		encoded, contentType, err = replaceTranscriptionModel(raw, contentType, d.Model)
	} else {
		var body map[string]json.RawMessage
		if json.Unmarshal(raw, &body) != nil || body == nil {
			err = errors.New("invalid hosted request")
		} else {
			body["model"], _ = json.Marshal(d.Model)
			stream = bytes.Equal(bytes.TrimSpace(body["stream"]), []byte("true"))
			if stream && r.URL.Path == "/v1/chat/completions" {
				options := map[string]json.RawMessage{}
				if rawOptions := body["stream_options"]; len(rawOptions) > 0 && (json.Unmarshal(rawOptions, &options) != nil || options == nil) {
					report.Status = "failed"
					http.Error(w, "invalid stream options", 400)
					return
				}
				options["include_usage"] = json.RawMessage("true")
				body["stream_options"], _ = json.Marshal(options)
			}
			encoded, err = json.Marshal(body)
		}
	}
	if err != nil {
		report.Status = "failed"
		http.Error(w, "invalid hosted request", 400)
		return
	}
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(encoded))
	if err != nil {
		report.Status = "failed"
		http.Error(w, "hosted request invalid", 502)
		return
	}
	up.GetBody = nil
	up.Header.Set("Content-Type", contentType)
	if r.URL.Path == "/v1/messages" {
		up.Header.Set("X-Api-Key", d.APIKey)
		up.Header.Set("Anthropic-Version", "2023-06-01")
		if beta != "" {
			up.Header.Set("Anthropic-Beta", beta)
		}
	} else {
		up.Header.Set("Authorization", "Bearer "+d.APIKey)
	}
	var meterErr error
	meter, meterErr = newHostedMeter(d.Model, r.URL.Path, encoded)
	if meterErr != nil {
		report.Status = "failed"
		http.Error(w, "hosted observation unavailable", http.StatusBadGateway)
		return
	}
	if err = meter.checkpoint(ctx, c, h, account.ID, d, &report); err != nil {
		http.Error(w, "hosted checkpoint unavailable; request was not replayed", http.StatusServiceUnavailable)
		return
	}
	client := policy.client(0) // the API absolute deadline owns both header and body waits
	defer client.CloseIdleConnections()
	response, err := client.Do(up)
	if err != nil {
		if r.Context().Err() != nil {
			report.Status = "cancelled"
		}
		http.Error(w, "hosted execution outcome unknown; request was not replayed", 502)
		return
	}
	defer func() {
		if closeHostedTransport(response.Body, cancel, &report) != nil {
			log.Printf("hosted_upstream_close_failed request=%s", d.RequestID)
		}
	}()
	report.TransportConfirmed = true
	if isImagePath(r.URL.Path) && boundImageResponse(response) != nil {
		report.Status = "failed"
		if r.Context().Err() != nil {
			report.Status = "cancelled"
		}
		http.Error(w, "image response exceeds limit or is not supported", http.StatusBadGateway)
		return
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		report.Status = "failed"
		http.Error(w, "hosted upstream redirect rejected", 502)
		return
	}
	if response.StatusCode >= 400 {
		report.Status = "failed"
		if isImagePath(r.URL.Path) {
			failure, readErr := io.ReadAll(io.LimitReader(response.Body, 8193))
			if readErr == nil && len(failure) <= 8192 {
				meter.usage.parse(failure)
				meter.observed = true
				meter.terminal = true
				if e := meter.checkpoint(ctx, c, h, account.ID, d, &report); e != nil {
					log.Printf("image_failure_checkpoint_pending request=%s", d.RequestID)
				}
			}
		}
		if retryAfter := response.Header.Get("Retry-After"); retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		status := http.StatusBadGateway
		if response.StatusCode == http.StatusTooManyRequests {
			status = http.StatusTooManyRequests
		}
		http.Error(w, "hosted upstream request failed", status)
		return
	}
	responseBody, actualStream, responseErr := hostedResponseReader(response, r.URL.Path, stream)
	if responseErr != nil {
		if errors.Is(responseErr, errHostedResponseInvalid) {
			report.Status = "failed"
		}
		if r.Context().Err() != nil {
			report.Status = "cancelled"
		}
		http.Error(w, "Hosted upstream response failed", http.StatusBadGateway)
		return
	}
	stream = actualStream
	meter.response(stream, response.Header.Get("Content-Type"))
	for _, name := range []string{"Content-Type", "Retry-After"} {
		if value := response.Header.Get(name); value != "" {
			w.Header().Set(name, value)
		}
	}
	capture := newUsageCapture(stream)
	capture.protocol = r.URL.Path
	filter := hostedSSEFilter{protocol: r.URL.Path}
	buffer := make([]byte, 32768)
	complete := false
	started := false
	observationFailed := false
	for {
		n, readErr := responseBody.Read(buffer)
		var output []byte
		var observeErr error
		if n > 0 {
			timing.Observe(buffer[:n], stream, time.Now())
			capture.feed(buffer[:n])
			output, observeErr = meter.feed(buffer[:n])
		}
		if readErr == io.EOF && observeErr == nil {
			var tail []byte
			tail, observeErr = meter.finish()
			output = append(output, tail...)
		}
		if len(output) > 0 || observeErr != nil || readErr != nil {
			if err = meter.checkpoint(ctx, c, h, account.ID, d, &report); err != nil {
				cancel()
				if r.Context().Err() != nil {
					report.Status = "cancelled"
				}
				if !started {
					http.Error(w, "hosted checkpoint unavailable", http.StatusServiceUnavailable)
				}
				break
			}
		}
		if observeErr != nil {
			observationFailed = true
			output = nil
			if stream {
				// Only a fixed protocol error may be returned after an invalid event;
				// never release unvalidated/truncated provider bytes.
				output = filter.failure()
				filter.uncertain = errors.Is(observeErr, metering.ErrTruncated)
			}
		}
		if stream && observeErr == nil {
			output = filter.feed(output)
			if readErr == io.EOF {
				output = append(output, filter.finish()...)
			}
		}
		if len(output) > 0 {
			if !started {
				w.WriteHeader(response.StatusCode)
				started = true
			}
			if _, err = w.Write(output); err != nil {
				report.Status = "cancelled"
				break
			}
			if stream {
				if err = http.NewResponseController(w).Flush(); err != nil {
					report.Status = "cancelled"
					break
				}
			}
		}
		if filter.failed {
			break
		}
		if observationFailed {
			break
		}
		if readErr != nil {
			complete = readErr == io.EOF
			break
		}
	}
	capture.counts()
	if response.StatusCode >= 400 || capture.invalid || filter.failed && !filter.uncertain {
		report.Status = "failed"
	} else if complete && (!stream || capture.completed && !capture.invalid) {
		report.Status = "succeeded"
		report.TransportConfirmed = true
	} else if r.Context().Err() != nil {
		report.Status = "cancelled"
	}
	if !started && report.Status == "succeeded" {
		w.WriteHeader(response.StatusCode)
	}
	if !started && observationFailed {
		http.Error(w, "Hosted upstream response failed", http.StatusBadGateway)
	}
}

func (h *chatHistory) hostedPending(account, requestID string) bool {
	var count int
	if h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox WHERE request_id=? AND account=?", requestID, account).Scan(&count) != nil {
		return true
	}
	return count > 0
}

func hostedUsagePayload(report usageReport) map[string]any {
	payload := map[string]any{"status": report.Status, "transportConfirmed": report.TransportConfirmed}
	if report.ImageUsage != nil {
		payload["imageUsage"] = report.ImageUsage
	}
	if report.ImageMetadata != nil {
		payload["imageMetadata"] = report.ImageMetadata
	}
	if report.MeterSequence > 0 {
		payload["meterSequence"] = report.MeterSequence
		payload["executionClosed"] = true
	} else {
		payload["inputTokens"], payload["outputTokens"], payload["cacheWriteTokens"], payload["cachedInputTokens"] = report.Input, report.Output, report.Write, report.Cached
	}
	if report.DurationMs > 0 {
		payload["durationMs"] = report.DurationMs
	}
	if report.FirstTokenMs != nil {
		payload["firstTokenMs"] = report.FirstTokenMs
	}
	if report.OutputTps != nil {
		payload["outputTps"] = report.OutputTps
	}
	return payload
}
