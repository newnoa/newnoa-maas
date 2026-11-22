package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestImageHistorySnapshot(t *testing.T) {
	value := map[string]any{"protocol": imageGenerationPath, "modelType": "image", "response": `{"data":[{"b64_json":"secret","url":"https://private/?token=secret"}],"usage":{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4}}`}
	recoverHistoryUsage(value)
	if value["imageUsage"] == nil || value["modelCategory"] != "image" {
		t.Fatalf("missing image snapshot: %v", value)
	}
	api := map[string]any{"imageCosts": map[string]any{"input": "1.0"}, "imagePricing": map[string]any{"version": 1}, "modelCategory": "image"}
	mergeHistoryTrustedMetadata(value, api)
	if value["imageCosts"] == nil || value["imagePricing"] == nil {
		t.Fatal("lost API snapshot")
	}
	raw, _ := json.Marshal(historyCapturePayload(value))
	var saved map[string]any
	_ = json.Unmarshal(raw, &saved)
	if saved["imageUsage"] == nil || saved["imageCosts"] == nil || saved["imagePricing"] == nil {
		t.Fatal("capture empty")
	}
}
func TestImageHistoryCategorySQL(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	store, err := openHistoryRepository(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, kind := range []string{"image", "llm", "vlm", "asr", ""} {
		record := map[string]any{"id": "[TEST]" + kind, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelType": kind, "request": "{}"}
		if err = store.Save(ctx, "viewer", "own", record); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		category string
		want     int64
	}{{"image", 1}, {"text", 3}, {"audio", 1}, {"", 5}} {
		page, e := store.Query(ctx, "viewer", historyQuery{ModelCategory: tc.category})
		if e != nil {
			t.Fatal(e)
		}
		if page.Total != tc.want {
			t.Fatalf("%s total=%d", tc.category, page.Total)
		}
	}
}

func TestImageHistoryProjectionUpgrade(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	repository, err := openHistoryRepository(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	store := repository.(*sqlHistoryRepository)
	value := map[string]any{"id": "[TEST] old-image", "model": "[TEST] image-model", "deploymentId": "[TEST] image-deployment", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelType": "image", "request": "{}"}
	if err = store.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec("UPDATE nn_history_records SET filter_version=1"); err != nil {
		t.Fatal(err)
	}
	if err = store.initializeFilters(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := store.Query(ctx, "viewer", historyQuery{ModelCategory: "image"})
	if err != nil || page.Total != 1 {
		t.Fatalf("upgrade: %+v %v", page, err)
	}
	filters, err := store.Filters(ctx, "viewer", historyFilterQuery{ModelCategory: "image", Kind: "models"})
	if err != nil {
		t.Fatal(err)
	}
	if len(filters.Items) != 1 || filters.Items[0].ID != "[TEST] image-deployment" {
		t.Fatalf("image options lost: %+v", filters)
	}
	textFilters, err := store.Filters(ctx, "viewer", historyFilterQuery{ModelCategory: "text", Kind: "models"})
	if err != nil || len(textFilters.Items) != 0 {
		t.Fatalf("image leaked to text options: %+v %v", textFilters, err)
	}
}

func TestImageHistorySettledSnapshotDoesNotRegress(t *testing.T) {
	value := map[string]any{"billingPhase": "final", "pointsStatus": "known", "imageCosts": map[string]any{"input": "1.0"}, "imageUsage": map[string]any{"textInputTokens": 12}, "imagePricing": map[string]any{"version": 1}}
	mergeHistoryTrustedMetadata(value, map[string]any{"billingPhase": "pending", "pointsStatus": "pending", "imageCosts": nil, "imageUsage": nil, "imagePricing": nil})
	if value["imageUsage"] == nil || value["imagePricing"] == nil || value["imageCosts"] == nil {
		t.Fatal("final snapshot regressed")
	}
}

func TestHostedImageMeterCarriesFiveCounters(t *testing.T) {
	meter, err := newHostedMeter("image", imageGenerationPath, []byte(`{"size":"1024x1024","quality":"high"}`))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"data":[{"url":"https://private/?token=secret"}],"usage":{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2,"cached_tokens":3,"cached_tokens_details":{"text_tokens":2,"image_tokens":1}},"output_tokens":4}}`)
	if _, err = meter.feed(raw); err != nil {
		t.Fatal(err)
	}
	if _, err = meter.finish(); err != nil {
		t.Fatal(err)
	}
	snapshot := meter.snapshot()
	if !snapshot.ImageUsage.Complete() || *snapshot.ImageUsage.CachedImageInputTokens != 1 || snapshot.ImageMetadata["size"] != "1024x1024" {
		t.Fatalf("snapshot %+v", snapshot)
	}
	report := usageReport{}
	meter.copyReported(&report)
	payload := hostedUsagePayload(report)
	if payload["imageUsage"] == nil {
		t.Fatal("final missing counters")
	}
}

func TestImageHistoryExportUsesCategoryAndAPISnapshot(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, kind := range []string{"image", "vlm"} {
		value := map[string]any{"id": "[TEST] " + kind, "modelType": kind, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "imageUsage": map[string]any{"textInputTokens": 0, "cachedTextInputTokens": 0, "imageInputTokens": 2, "cachedImageInputTokens": nil, "imageOutputTokens": 4}, "imagePricing": map[string]any{"version": 3}, "imageCosts": map[string]any{"input": "0.000000000"}, "settlementStatus": "pending", "pointsConsumed": nil}
		if err = store.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
	}
	for _, format := range []string{"json", "csv"} {
		out, e := writeHistoryExport(ctx, store, "viewer", historyQuery{ModelCategory: "image"}, format, t.TempDir(), nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		if out.Count != 1 {
			t.Fatalf("export includes other category: %+v", out)
		}
		data, e := os.ReadFile(out.Path)
		if e != nil {
			t.Fatal(e)
		}
		if format == "csv" {
			rows, e := csv.NewReader(strings.NewReader(string(data))).ReadAll()
			if e != nil {
				t.Fatal(e)
			}
			for i, key := range rows[0] {
				if key == "imageUsage" {
					var u map[string]any
					if e = json.Unmarshal([]byte(rows[1][i]), &u); e != nil || u["textInputTokens"] != float64(0) || u["cachedImageInputTokens"] != nil {
						t.Fatalf("CSV snapshot lost: %s %v", rows[1][i], e)
					}
				}
			}
		} else {
			var rows []map[string]any
			if e = json.Unmarshal(data, &rows); e != nil || len(rows) != 1 || rows[0]["imagePricing"] == nil || rows[0]["imageCosts"] == nil {
				t.Fatalf("JSON snapshot lost: %s %v", data, e)
			}
		}
	}
	other, e := store.Query(ctx, "other", historyQuery{ModelCategory: "image"})
	if e != nil || other.Total != 0 {
		t.Fatalf("account isolation: %+v %v", other, e)
	}
}

func TestImageCategoryValidation(t *testing.T) {
	query := historyQuery{ModelCategory: "video"}
	if query.validate() == nil {
		t.Fatal("invalid category accepted")
	}
	filter, e := mongoHistoryFilter("viewer", historyQuery{Scope: "own", ModelCategory: "text"}, false)
	if e != nil || filter["model_category"] != "text" {
		t.Fatalf("mongo filter: %v %v", filter, e)
	}
}
