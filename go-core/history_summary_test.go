package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHistoryContentSummary(t *testing.T) {
	for _, tc := range []struct{ name, request, response, want string }{
		{"chat latest user", `{"client_metadata":{"session_id":"hidden"},"messages":[{"role":"system","content":"instructions"},{"role":"user","content":"old"},{"role":"assistant","content":"old answer"},{"role":"user","content":"你好，请简单介绍一下自己。"}]}`, "", "你好，请简单介绍一下自己。"},
		{"responses input", `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"检查日志"},{"type":"input_image","image_url":"secret"}]}]}`, "", "检查日志"},
		{"string input", `{"input":"客户的问题"}`, "", "客户的问题"},
		{"image prompt", `{"prompt":"画一只猫"}`, "", "画一只猫"},
		{"anthropic blocks", `{"messages":[{"role":"user","content":[{"type":"text","text":"第一行"},{"type":"text","text":"第二行"}]}]}`, "", "第一行 第二行"},
		{"chat answer", `{"messages":[{"role":"system","content":"hidden"}]}`, `{"choices":[{"message":{"role":"assistant","content":"Agent 的回答"}}]}`, "Agent 的回答"},
		{"responses answer", `{}`, `{"output":[{"type":"reasoning","summary":[{"text":"hidden"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"完成了"}]}]}`, "完成了"},
		{"anthropic answer", `{}`, `{"role":"assistant","content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"回答"}]}`, "回答"},
		{"stream chat", `{}`, "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"世界\"}}]}\n\ndata: [DONE]\n", "你好世界"},
		{"stream responses", `{}`, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"回答\"}\n\n", "回答"},
		{"completed stream avoids duplicate answer", `{}`, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"回答\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"回答\"}]}]}}\n\n", "回答"},
		{"stream anthropic", `{}`, "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"回答\"}}\n\n", "回答"},
		{"request assistant fallback", `{"input":[{"role":"assistant","content":"上次的回答"},{"type":"function_call_output","output":"secret"}]}`, "", "上次的回答"},
		{"empty image question uses answer", `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"secret"}}]}]}`, `{"choices":[{"message":{"content":"这是一只猫"}}]}`, "这是一只猫"},
		{"plain text", "  hello\n world  ", "", "hello world"},
		{"metadata only", `{"client_metadata":{"session_id":"hidden"},"max_tokens":100}`, `{"error":{"message":"internal error"}}`, ""},
		{"malformed json", `{"messages":[`, "", ""},
		{"unicode bound", `{"input":"` + strings.Repeat("中", 130) + `"}`, "", strings.Repeat("中", 120)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := historyRecordMetadata(map[string]any{"request": tc.request, "response": tc.response, "contentSummary": "stale JSON"})
			if got := metadata["contentSummary"]; got != tc.want {
				t.Fatalf("summary = %q, want %q", got, tc.want)
			}
			if _, ok := metadata["request"]; ok {
				t.Fatal("request leaked to list")
			}
			if _, ok := metadata["response"]; ok {
				t.Fatal("response leaked to list")
			}
		})
	}
}

func TestHistoryQueryRefreshesOldSummary(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record := map[string]any{"id": "old", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": `{"client_metadata":{"session_id":"hidden"},"messages":[{"role":"user","content":"真正的问题"}]}`}
	if err := store.Save(ctx, "viewer", "own", record); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "viewer", "own", map[string]any{"id": "older", "createdAt": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), "request": "other"}); err != nil {
		t.Fatal(err)
	}
	old := map[string]any{"id": "old", "contentSummary": `{"client_metadata":`}
	raw, _ := json.Marshal(old)
	if _, err := store.(*sqlHistoryRepository).db.Exec("UPDATE nn_history_records SET metadata=? WHERE id='old'", string(raw)); err != nil {
		t.Fatal(err)
	}
	page, err := store.Query(ctx, "viewer", historyQuery{Scope: "own", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0]["contentSummary"] != "真正的问题" {
		t.Fatalf("old summary: %+v", page)
	}
	if page.NextCursor == "" {
		t.Fatal("pagination cursor lost")
	}
	if page.Items[0]["request"] != nil || page.Items[0]["response"] != nil {
		t.Fatal("body leaked to list")
	}
	detail, err := store.Get(ctx, "viewer", "own", "old")
	if err != nil || detail["request"] != record["request"] {
		t.Fatal("original content changed", err)
	}
}

func TestHistoryLegacySummaryUsesDialogue(t *testing.T) {
	journal, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.db.Close()
	store, err := openLegacyHistoryRepository(journal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	record := map[string]any{"id": "legacy", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": `{"client_metadata":{"session_id":"hidden"},"input":"旧记录的问题"}`}
	if err := store.Save(ctx, "viewer", "own", record); err != nil {
		t.Fatal(err)
	}
	page, err := store.Query(ctx, "viewer", historyQuery{Scope: "own", Limit: 20})
	if err != nil || len(page.Items) != 1 || page.Items[0]["contentSummary"] != "旧记录的问题" {
		t.Fatalf("legacy summary: %+v, %v", page, err)
	}
}

func TestModelTestHistoryLimitsOnlyMatchingSummaries(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now()
	rows := []struct{ id, key, deployment, agent, kind string }{
		{"legacy", "k", "", "NEWNOA Model Test", ""},
		{"matching", "k", "m", "NEWNOA Model Test", "llm"},
		{"rec", "k", "m", "NEWNOA Model Test", "recommendation"},
		{"foreign", "k2", "m", "NEWNOA Model Test", "llm"},
		{"unauthorized", "k", "m2", "NEWNOA Model Test", "llm"},
		{"external", "k", "m", "agent", "llm"},
	}
	for index, r := range rows {
		record := map[string]any{"id": r.id, "keyId": r.key, "deploymentId": r.deployment, "model": "m", "agent": r.agent, "modelType": r.kind, "createdAt": now.Add(time.Duration(index) * time.Second).UTC().Format(time.RFC3339Nano), "request": "private body"}
		if err := store.Save(ctx, "viewer", "own", record); err != nil {
			t.Fatal(err)
		}
	}
	q := historyQuery{ModelCategory: "text", TestModelType: "llm", TestGrants: []historyTestGrant{{"k", "m"}}, Limit: 1}
	first, err := store.Query(ctx, "viewer", q)
	if err != nil || first.Total != 2 || len(first.Items) != 1 || first.Items[0]["id"] != "matching" || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	q.Before = first.NextCursor
	next, err := store.Query(ctx, "viewer", q)
	if err != nil || len(next.Items) != 1 || next.Items[0]["id"] != "legacy" || next.Items[0]["request"] != nil || next.NextCursor != "" {
		t.Fatalf("legacy page: %+v %v", next, err)
	}
	// A legacy summary does not need a repository body lookup.
	stale := map[string]any{"id": "absent", "contentSummary": "raw request fragment"}
	if err := refreshHistorySummaries(ctx, nil, "viewer", q, []map[string]any{stale}); err != nil || stale["contentSummary"] != nil {
		t.Fatal(stale, err)
	}
}
