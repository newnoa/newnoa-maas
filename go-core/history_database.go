package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type historyDatabaseConfig struct {
	Engine          string `json:"engine"`
	ResumeEngine    string `json:"resumeEngine,omitempty"`
	Host            string `json:"host,omitempty"`
	Port            int    `json:"port,omitempty"`
	Database        string `json:"database,omitempty"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
	TLS             bool   `json:"tls"`
	AuthDatabase    string `json:"authDatabase,omitempty"`
	SQLiteDirectory string `json:"sqliteDirectory"`
	ExportDirectory string `json:"exportDirectory"`
	RetentionDays   int    `json:"retentionDays"`
}

func (c historyDatabaseConfig) sameConnection(other historyDatabaseConfig) bool {
	engine := c.Engine
	if engine == "off" {
		engine = c.ResumeEngine
	}
	return engine == other.Engine && c.Host == other.Host && c.Port == other.Port && c.Database == other.Database && c.Username == other.Username
}

func defaultHistoryDatabase(home string) historyDatabaseConfig {
	root := filepath.Join(home, ".newnoa")
	if dockerRuntime() {
		root = "/data"
	}
	return historyDatabaseConfig{Engine: "sqlite", SQLiteDirectory: filepath.Join(root, "sqlite"), ExportDirectory: filepath.Join(root, "exports"), Database: "newnoa_history", RetentionDays: 7}
}
func historyRetentionChoices(engine string) []int {
	if engine == "sqlite" {
		return []int{1, 2, 3, 4, 5, 6, 7}
	}
	return []int{7, 30, 90, 180, 365, 0}
}
func (c *historyDatabaseConfig) normalizeRetention() {
	if c.Engine == "sqlite" && (c.RetentionDays < 1 || c.RetentionDays > 7) {
		c.RetentionDays = 7
	}
}
func (c historyDatabaseConfig) validate() error {
	if len(c.Engine) > 16 || len(c.ResumeEngine) > 16 || utf8.RuneCountInString(c.Host) > 253 || utf8.RuneCountInString(c.Database) > 63 || utf8.RuneCountInString(c.Username) > 128 || utf8.RuneCountInString(c.Password) > 256 || utf8.RuneCountInString(c.AuthDatabase) > 128 || utf8.RuneCountInString(c.SQLiteDirectory) > 4096 || utf8.RuneCountInString(c.ExportDirectory) > 4096 {
		return errors.New("history database configuration exceeds field length limits")
	}
	if c.Engine == "off" {
		return nil
	}
	valid := false
	for _, days := range historyRetentionChoices(c.Engine) {
		valid = valid || c.RetentionDays == days
	}
	if !valid {
		return errors.New("invalid history retention for database")
	}
	switch c.Engine {
	case "sqlite":
		if !filepath.IsAbs(c.SQLiteDirectory) {
			return errors.New("SQLite directory must be absolute")
		}
	case "mongodb", "postgres", "mysql":
		if c.Host == "" || strings.ContainsAny(c.Host, "/\\@?#\x00\r\n") || c.Port < 1 || c.Port > 65535 || !regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,62}$`).MatchString(c.Database) || c.Username == "" {
			return errors.New("invalid history database connection")
		}
	default:
		return errors.New("unsupported history database")
	}
	if c.ExportDirectory != "" && !filepath.IsAbs(c.ExportDirectory) {
		return errors.New("Export directory must be absolute")
	}
	return nil
}
func historyDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) {
		return errors.New("invalid history directory")
	}
	current := filepath.Clean(path)
	for current != filepath.Dir(current) {
		if info, err := os.Lstat(current); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("history directory cannot contain symbolic links")
		}
		current = filepath.Dir(current)
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return errors.New("history directory cannot be created")
	}
	probe, err := os.CreateTemp(path, ".newnoa-write-check-")
	if err != nil {
		return errors.New("history directory is not writable")
	}
	name := probe.Name()
	closeErr := probe.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

type historyTestGrant struct {
	KeyID string `json:"keyId"`
	Model string `json:"model"`
}

type historyQuery struct {
	TestModelType  string             `json:"testModelType,omitempty"`
	TestGrants     []historyTestGrant `json:"testGrants,omitempty"`
	ModelCategory  string             `json:"modelCategory"`
	Scope          string             `json:"scope"`
	Q              string             `json:"q"`
	KeyName        string             `json:"keyName"`
	Model          string             `json:"model"`
	KeyID          string             `json:"keyId"`
	CallerID       string             `json:"callerId"`
	From           string             `json:"from"`
	To             string             `json:"to"`
	Before         string             `json:"before"`
	StorageID      string             `json:"storageId"`
	Limit          int                `json:"limit"`
	IncludeContent bool               `json:"-"`
}

func (q *historyQuery) validate() error {
	if q.ModelCategory != "" && q.ModelCategory != "text" && q.ModelCategory != "image" && q.ModelCategory != "audio" {
		return errors.New("invalid model category")
	}
	if q.Scope == "" {
		q.Scope = "own"
	}
	if q.Scope != "own" && q.Scope != "others" {
		return errors.New("invalid history scope")
	}
	if q.TestModelType != "" || len(q.TestGrants) != 0 {
		if (q.ModelCategory != "" && q.ModelCategory != "text") || (q.TestModelType != "llm" && q.TestModelType != "recommendation") || len(q.TestGrants) == 0 || len(q.TestGrants) > 64 || q.Scope != "own" || q.Q != "" || q.IncludeContent {
			return errors.New("invalid model test history query")
		}
		for _, grant := range q.TestGrants {
			if grant.KeyID == "" || grant.Model == "" || len(grant.KeyID) > 200 || len(grant.Model) > 200 {
				return errors.New("invalid model test history grant")
			}
		}
	}
	if q.Limit == 0 {
		q.Limit = 50
	}
	if q.Limit < 1 || q.Limit > 100 || len(q.Q) > 500 || len(q.KeyName) > 200 || len(q.CallerID) > 200 || len(q.StorageID) > 200 || len(q.Model) > 200 || len(q.KeyID) > 200 {
		return errors.New("invalid history query")
	}
	for _, stamp := range []string{q.From, q.To} {
		if stamp != "" {
			if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
				return errors.New("invalid history date range")
			}
		}
	}
	if q.From != "" && q.To != "" {
		from, _ := time.Parse(time.RFC3339Nano, q.From)
		to, _ := time.Parse(time.RFC3339Nano, q.To)
		if from.After(to) {
			return errors.New("invalid history date range")
		}
	}
	return nil
}

type historyQueryResult struct {
	Items      []map[string]any `json:"items"`
	NextCursor string           `json:"nextCursor,omitempty"`
	Total      int64            `json:"total"`
	ElapsedMs  int64            `json:"elapsedMs"`
	Database   string           `json:"database"`
}
type historyRepository interface {
	Save(context.Context, string, string, map[string]any) error
	Enrich(context.Context, string, string, string, map[string]any) error
	Query(context.Context, string, historyQuery) (historyQueryResult, error)
	Filters(context.Context, string, historyFilterQuery) (historyFilterResult, error)
	Get(context.Context, string, string, string) (map[string]any, error)
	Delete(context.Context, string, string, string) error
	Candidate(context.Context, string, string, string, bool) error
	Accounts(context.Context, string, string, int, ...string) ([]map[string]any, error)
	CandidateStates(context.Context, string, string, int) ([]map[string]any, error)
	Expire(context.Context, string, int) error
	Version() string
	Close() error
}

func openHistoryRepository(ctx context.Context, c historyDatabaseConfig) (historyRepository, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if c.Engine == "off" {
		return &disabledHistoryRepository{unavailableHistoryRepository{err: errHistoryDisabled}}, nil
	}
	if c.Engine == "mongodb" {
		return openMongoHistory(ctx, c)
	}
	return openSQLHistory(ctx, c)
}
func historyText(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}
func historyCursor(stamp, id string) string {
	raw, _ := json.Marshal([]string{stamp, id})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func parseHistoryCursor(cursor string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", errors.New("invalid history cursor")
	}
	var pair []string
	if json.Unmarshal(raw, &pair) != nil || len(pair) != 2 {
		return "", "", errors.New("invalid history cursor")
	}
	return pair[0], pair[1], nil
}
func historyEndpoint(c historyDatabaseConfig) string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}
func historyRecordMetadata(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, v := range value {
		if key != "request" && key != "response" {
			result[key] = v
		}
	}
	if value["request"] != nil || value["response"] != nil {
		result["contentSummary"] = historyContentSummary(historyText(value["request"]), historyText(value["response"]))
		result["contentSummaryVersion"] = historySummaryVersion
	}
	return result
}

// Match Java API ChatHistory.FIELDS; trusted display snapshots stay local and are re-derived by API.
func historyCapturePayload(value map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range []string{"id", "createdAt", "finishedAt", "durationMs", "model", "agent", "keyId", "protocol", "request", "response", "requestTruncated", "responseTruncated", "httpStatus", "interrupted", "contentType", "transportPath", "requestId", "conversationId", "storageId", "storageMode", "modelType", "attachments", "inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens", "pointsConsumed", "pointsStatus", "modelCategory", "imageUsage", "imagePricing", "imageCosts", "imageMetadata", "callerSource", "reasoningEffort", "clientIp", "firstTokenMs", "outputTps"} {
		if field, ok := value[key]; ok {
			result[key] = field
		}
	}
	return result
}
