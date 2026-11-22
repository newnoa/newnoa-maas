package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const historyManagerSchema = `CREATE TABLE IF NOT EXISTS history_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS history_viewers(viewer TEXT PRIMARY KEY); CREATE TABLE IF NOT EXISTS history_local_settings(viewer TEXT PRIMARY KEY,enabled INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS history_pending(seq INTEGER PRIMARY KEY AUTOINCREMENT,viewer TEXT NOT NULL,scope TEXT NOT NULL,kind TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL); CREATE TABLE IF NOT EXISTS history_sync_cursors(viewer TEXT NOT NULL,scope TEXT NOT NULL,engine TEXT NOT NULL,cursor TEXT NOT NULL,PRIMARY KEY(viewer,scope,engine));`

type historyJob struct {
	JobID        string `json:"jobId"`
	State        string `json:"state"`
	Percent      int    `json:"percent"`
	Error        string `json:"error,omitempty"`
	LastSyncedAt string `json:"lastSyncedAt,omitempty"`
	Path         string `json:"path,omitempty"`
	Filename     string `json:"filename,omitempty"`
	Count        int64  `json:"count"`
	Bytes        int64  `json:"bytes"`
	owner        string
	kind         string
	scope        string
	cancel       context.CancelFunc
	done         chan struct{}
}

func (j historyJob) publicStatus() historyJob {
	method := "history.sync.status"
	switch j.kind {
	case "export":
		method = "history.export.status"
	case "migration":
		method = "history.database.configure"
	}
	j.Error = publicStatusError(method, j.Error)
	return j
}

type historyRuntime struct {
	notify   func(account string, mask int)
	mu       sync.Mutex
	jobs     map[string]*historyJob
	syncJobs map[string]string
	busy     string
}
type historyManager struct {
	*historyRuntime
	childMu      sync.Mutex
	childrenMu   sync.RWMutex
	initializing map[string]bool
	initWG       sync.WaitGroup
	closing      bool
	children     map[string]*historyManager
	owner        string
	coordinator  bool
	store        historyRepository
	config       historyDatabaseConfig
	journal      *chatHistory
	home         string
	migration    string
	latestExport string
	migrating    bool
}

func (m *historyManager) notifyChange(account string, mask int) {
	if m.notify != nil {
		m.notify(account, mask)
	}
}

func (m *historyManager) notifyJob(job *historyJob) {
	mask := historyJobsChanged
	switch job.kind {
	case "export":
		mask = historyExportChanged
	case "migration":
		mask = historySettingsChanged
	}
	if job.State == "complete" {
		mask |= historyRecordsChanged
	}
	m.notifyChange(job.owner, mask)
}

func (m *historyManager) configPath() string {
	return m.privatePath("history-database.json")
}
func newHistoryManager(home string, h *chatHistory) (*historyManager, error) {
	return newAccountHistoryManager(home, h, "", nil, false)
}
func newHistoryCoordinator(home string, h *chatHistory) (*historyManager, error) {
	return newAccountHistoryManager(home, h, "", nil, true)
}
func newAccountHistoryManager(home string, h *chatHistory, owner string, runtime *historyRuntime, coordinator bool) (*historyManager, error) {
	if runtime == nil {
		runtime = &historyRuntime{jobs: map[string]*historyJob{}, syncJobs: map[string]string{}}
	}
	m := &historyManager{home: home, journal: h, config: defaultHistoryDatabase(home), historyRuntime: runtime, owner: owner, coordinator: coordinator, children: map[string]*historyManager{}, initializing: map[string]bool{}}
	_, err := h.db.Exec(historyManagerSchema)
	if err == nil {
		err = initializeCategoryPending(h.db.DB)
	}
	if err != nil {
		return nil, err
	}
	if coordinator {
		return m, nil
	}
	if owner != "" {
		m.config.Engine, m.config.ResumeEngine = "off", "sqlite"
		if _, err = h.db.Exec("INSERT OR IGNORE INTO history_viewers(viewer) VALUES(?)", owner); err != nil {
			return nil, err
		}
	}

	if info, err := os.Lstat(m.configPath()); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return nil, errors.New("unsafe history configuration")
	}
	if raw, err := os.ReadFile(m.configPath()); err == nil {
		var config historyDatabaseConfig
		if json.Unmarshal(raw, &config) != nil {
			return nil, errors.New("invalid history database configuration")
		}
		m.config = config
		m.config.normalizeRetention()
		if config != m.config {
			if err = m.preserveHistoryCursors(context.Background(), config, m.config); err != nil {
				return nil, err
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := historyDirectory(m.config.ExportDirectory); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	m.store, err = openHistoryRepository(ctx, m.config)
	if err != nil {
		m.store = &unavailableHistoryRepository{err: err}
	}
	if m.config.Engine == "sqlite" {
		m.mu.Lock()
		if m.busy == "" {
			if e := m.maintainSQLite(ctx, time.Now()); e != nil {
				m.mu.Unlock()
				m.store.Close()
				return nil, e
			}
		}
		m.mu.Unlock()
	}
	if owner != "" && m.config.Engine != "off" {
		var imported string
		h.db.QueryRow("SELECT value FROM history_meta WHERE key=?", "legacy-imported:"+owner).Scan(&imported)
		var count int
		h.db.QueryRow("SELECT count(*) FROM exchanges WHERE account=? AND deleted_at IS NULL", owner).Scan(&count)
		if imported != "1" && count > 0 {
			target := m.config
			m.store.Close()
			legacy, e := openLegacyHistoryRepository(h)
			if e != nil {
				return nil, e
			}
			m.store = legacy
			m.config = defaultHistoryDatabase(home)
			if err = m.loadJob(); err != nil {
				return nil, err
			}
			if raw, e := os.ReadFile(m.privatePath("history-migration.json")); e == nil {
				var resume struct {
					Config historyDatabaseConfig `json:"config"`
				}
				if json.Unmarshal(raw, &resume) != nil {
					return nil, errors.New("invalid interrupted legacy history migration")
				}
				resume.Config.normalizeRetention()
				if resume.Config.validate() != nil {
					return nil, errors.New("invalid interrupted legacy history migration")
				}
				target = resume.Config
			}
			jobCtx, id, e := m.start("migration", owner)
			if e != nil {
				return nil, e
			}
			m.migration = id
			go func() {
				err := m.migrate(jobCtx, target)
				if err == nil {
					_, err = h.db.Exec("INSERT OR REPLACE INTO history_meta(key,value) VALUES(?,'1')", "legacy-imported:"+owner)
				}
				m.finish(id, err, historyExportResult{})
			}()
			return m, nil
		}
	}
	if _, unavailable := m.store.(*unavailableHistoryRepository); !unavailable {
		if err = m.restore(ctx); err != nil {
			m.store.Close()
			return nil, err
		}
	}

	if err = m.loadJob(); err != nil {
		m.store.Close()
		return nil, err
	}
	resumePath := m.privatePath("history-migration.json")
	if info, e := os.Lstat(resumePath); e == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		raw, e := os.ReadFile(resumePath)
		var resume struct {
			Config historyDatabaseConfig `json:"config"`
			Owner  string                `json:"owner"`
		}
		if e != nil || json.Unmarshal(raw, &resume) != nil {
			m.store.Close()
			return nil, errors.New("invalid interrupted history migration")
		}
		resume.Config.normalizeRetention()
		if resume.Config.validate() != nil {
			m.store.Close()
			return nil, errors.New("invalid interrupted history migration")
		}
		jobCtx, id, e := m.start("migration", resume.Owner)
		if e != nil {
			m.store.Close()
			return nil, e
		}
		m.migration = id
		go func() { err := m.migrate(jobCtx, resume.Config); m.finish(id, err, historyExportResult{}) }()
	}
	m.mu.Lock()
	if s, ok := m.store.(*sqlHistoryRepository); ok && m.busy == "" && owner != "" {
		if err = s.recoverUsage(ctx, owner); err != nil {
			m.mu.Unlock()
			m.store.Close()
			return nil, err
		}
	}
	m.mu.Unlock()
	return m, nil
}
func (m *historyManager) enabled(viewer string) bool {
	var enabled bool
	err := m.journal.db.QueryRow("SELECT enabled FROM history_local_settings WHERE viewer=?", viewer).Scan(&enabled)
	return err == sql.ErrNoRows || err == nil && enabled
}
func (m *historyManager) apply(ctx context.Context, viewer, scope, kind, id string, value map[string]any) error {
	if _, err := m.journal.db.ExecContext(ctx, "INSERT OR IGNORE INTO history_viewers(viewer) VALUES(?)", viewer); err != nil {
		return err
	}
	switch kind {
	case "record":
		if m.config.expiresRecord(value, time.Now()) {
			return nil
		}
		if err := m.store.Save(ctx, viewer, scope, value); err != nil {
			return err
		}
		return m.store.Enrich(ctx, viewer, scope, id, value)
	case "delete":
		if scope == "own" {
			m.journal.bodyMu.Lock()
			_, err := m.journal.db.ExecContext(ctx, "UPDATE exchanges SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP),pending=0 WHERE account=? AND id=?", viewer, id)
			if err == nil {
				err = m.journal.retireDeletedJournalBodyLocked(ctx, viewer, id)
			}
			m.journal.bodyMu.Unlock()
			if err != nil {
				return err
			}
		}
		return m.store.Delete(ctx, viewer, scope, id)
	case "candidate":
		active, _ := value["active"].(bool)
		return m.store.Candidate(ctx, viewer, id, historyText(value["email"]), active)
	}
	return errors.New("invalid history event")
}
func applyHistoryEvent(ctx context.Context, s historyRepository, viewer, scope, kind, id, raw string) error {
	var value map[string]any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return errors.New("invalid history journal")
	}
	switch kind {
	case "record":
		if s, ok := s.(*sqlHistoryRepository); ok && s.engine == "sqlite" && expiredHistoryAtCutoff(value, s.retentionCutoff(ctx)) {
			return nil
		}
		if err := s.Save(ctx, viewer, scope, value); err != nil {
			return err
		}
		return s.Enrich(ctx, viewer, scope, id, value)
	case "delete":
		return s.Delete(ctx, viewer, scope, id)
	case "candidate":
		active, _ := value["active"].(bool)
		return s.Candidate(ctx, viewer, id, historyText(value["email"]), active)
	}
	return errors.New("invalid history event")
}
func (m *historyManager) commit(ctx context.Context, viewer, scope, kind, id string, value map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitLocked(ctx, viewer, scope, kind, id, value)
}

func (m *historyManager) commitLocked(ctx context.Context, viewer, scope, kind, id string, value map[string]any) error {
	if m.owner != "" && viewer != m.owner {
		return errors.New("history account mismatch")
	}
	if m.config.Engine == "off" || kind == "record" && m.config.expiresRecord(value, time.Now()) {
		return nil
	}
	m.journal.bodyMu.Lock()
	encoded := ""
	var err error
	if kind == "record" {
		encoded, err = m.journal.encodeBody(ctx, "pending", viewer, scope, id, value)
	}
	var seq int64
	if err == nil {
		seq, err = queueEncodedHistoryEvent(ctx, m.journal.db.DB, viewer, scope, kind, id, value, encoded)
	}
	m.journal.bodyMu.Unlock()
	if err != nil {
		return err
	}

	if job := m.jobs[m.busy]; job != nil && job.kind == "export" && job.owner == viewer {
		return nil
	}
	if err = m.apply(ctx, viewer, scope, kind, id, value); err != nil {
		return err
	}
	m.notifyChange(viewer, historyRecordsChanged)
	if !m.migrating {
		_, err = m.journal.db.ExecContext(ctx, "DELETE FROM history_pending WHERE seq=?", seq)
	}
	return err
}
func (m *historyManager) replay(ctx context.Context, target historyRepository, after int64) error {
	return m.replayThrough(ctx, target, after, 1<<63-1)
}
func (m *historyManager) replayThrough(ctx context.Context, target historyRepository, after, upper int64) error {
	for {
		var seq int64
		var viewer, scope, kind, id, raw string
		err := m.journal.db.QueryRowContext(ctx, "SELECT seq,viewer,scope,kind,id,payload FROM history_pending WHERE seq>? AND seq<=? AND (?='' OR viewer=?) ORDER BY seq LIMIT 1", after, upper, m.owner, m.owner).Scan(&seq, &viewer, &scope, &kind, &id, &raw)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err = resolveHistoryEvent(ctx, m.journal.db.DB, seq, raw)
		if err != nil {
			return err
		}
		expanded, decodeErr := m.journal.decodePending(ctx, viewer, raw)
		if decodeErr != nil {
			return decodeErr
		}
		if err = applyHistoryEvent(ctx, target, viewer, scope, kind, id, expanded); err != nil {
			return err
		}
		after = seq
	}
}
func (m *historyManager) masked() map[string]any {
	raw, _ := json.Marshal(m.config)
	var value map[string]any
	json.Unmarshal(raw, &value)
	delete(value, "password")
	value["passwordConfigured"] = m.config.Password != ""
	value["version"] = m.store.Version()
	value["allowedRetentionDays"] = historyRetentionChoices(m.config.Engine)
	value["migration"] = historyJob{State: "idle"}
	if job := m.jobs[m.migration]; job != nil {
		value["migration"] = job.publicStatus()
	}
	value["export"] = historyJob{State: "idle"}
	if job := m.jobs[m.latestExport]; job != nil && job.owner == m.owner {
		value["export"] = job.publicStatus()
	}
	return value
}
func (m *historyManager) start(kind string, owner ...string) (context.Context, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.Engine == "off" && kind != "migration" {
		return nil, "", errHistoryDisabled
	}
	if m.busy != "" {
		return nil, "", errors.New("history synchronization, migration or export already running")
	}
	id := randomToken()
	ctx, cancel := context.WithCancel(context.Background())
	m.jobs[id] = &historyJob{JobID: id, State: "running", cancel: cancel, done: make(chan struct{})}
	m.jobs[id].kind = kind
	if kind == "sync" {
		m.jobs[id].scope = "own"
		if len(owner) > 1 {
			m.jobs[id].scope = owner[1]
		}
	}
	if kind == "export" {
		m.latestExport = id
	}
	if len(owner) > 0 {
		m.jobs[id].owner = owner[0]
	}
	if kind == "sync" {
		m.journal.db.QueryRow("SELECT value FROM history_meta WHERE key=?", "last-sync:"+m.jobs[id].owner+":"+m.jobs[id].scope).Scan(&m.jobs[id].LastSyncedAt)
	}
	m.busy = id
	if err := m.persistJob(m.jobs[id]); err != nil {
		cancel()
		delete(m.jobs, id)
		m.busy = ""
		return nil, "", err
	}
	m.notifyJob(m.jobs[id])
	return ctx, id, nil
}
func (m *historyManager) progress(id string, percent int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job := m.jobs[id]; job != nil {
		if job.Percent >= min(99, percent) {
			return
		}
		job.Percent = min(99, percent)
		m.notifyJob(job)
		if err := m.persistJob(job); err != nil {
			job.Error = "history progress cannot be persisted"
			job.cancel()
		}
	}
}
func (m *historyManager) finish(id string, err error, result historyExportResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil {
		return
	}
	defer m.notifyJob(job)
	if job.done != nil {
		defer close(job.done)
	}
	if job.kind == "export" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		replayErr := m.replay(ctx, m.store, 0)
		if replayErr == nil {
			_, replayErr = m.journal.db.ExecContext(ctx, "DELETE FROM history_pending WHERE (?='' OR viewer=?)", m.owner, m.owner)
		}
		cancel()
		if replayErr != nil {
			err = errors.Join(err, replayErr)
		}
	}
	job.cancel()
	job.State = "complete"
	if err != nil {
		job.State = "failed"
		job.Error = err.Error()
		if errors.Is(err, context.Canceled) {
			job.State = "cancelled"
		}
	} else {
		job.Percent = 100
		job.Path = result.Path
		job.Filename = result.Filename
		job.Count = result.Count
		job.Bytes = result.Bytes
		if job.kind == "sync" {
			job.LastSyncedAt = time.Now().UTC().Format(time.RFC3339)
			if _, e := m.journal.db.Exec("INSERT OR REPLACE INTO history_meta(key,value) VALUES(?,?)", "last-sync:"+job.owner+":"+job.scope, job.LastSyncedAt); e != nil {
				job.State = "failed"
				job.Percent = 99
				job.Error = "history synchronization completion cannot be persisted"
			}
		}
	}
	if m.busy == id {
		m.busy = ""
	}
	if e := m.persistJob(job); e != nil {
		job.State = "failed"
		job.Percent = min(job.Percent, 99)
		job.Error = "history completion cannot be persisted"
	}
}
func (m *historyManager) migrate(ctx context.Context, target historyDatabaseConfig) error {
	// Keep source reads, target admission and count checks on the same rolling-retention snapshot.
	now := historyRetentionTime(ctx)
	ctx = context.WithValue(ctx, historyRetentionContextKey{}, now)
	targetCutoff := historyCutoff(now, target.RetentionDays)
	if err := target.validate(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.config.Engine == "off" || target.Engine == "off" {
		defer m.mu.Unlock()
		previous := m.config
		previous.Engine, previous.ResumeEngine = previous.ResumeEngine, ""
		previous.normalizeRetention()
		if previous.Engine == "sqlite" && target.Engine == "sqlite" && filepath.Clean(previous.SQLiteDirectory) == filepath.Clean(target.SQLiteDirectory) {
			if previous.RetentionDays < target.RetentionDays {
				// Clear records that elapsed under the suspended policy before opening with a longer one.
				store, err := openHistoryRepository(ctx, previous)
				if err != nil {
					return err
				}
				offConfig, offStore := m.config, m.store
				m.config, m.store = previous, store
				err = m.maintainSQLite(ctx, now)
				m.config, m.store = offConfig, offStore
				store.Close()
				if err != nil {
					return err
				}
			}
			if err := m.preserveHistoryCursors(ctx, previous, target); err != nil {
				return err
			}
		}
		if err := m.switchRecording(target); err != nil {
			return err
		}
		if target.Engine == "sqlite" {
			if err := m.maintainSQLite(ctx, now); err != nil {
				return err
			}
		}
		return m.restore(ctx)
	}
	previous := m.config
	if previous.Engine == "sqlite" && target.Engine == "sqlite" && target.RetentionDays > previous.RetentionDays {
		// Expire using the old policy before extending, so elapsed records cannot reappear.
		if err := m.maintainSQLite(ctx, now); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	if previous.Engine == "sqlite" && target.Engine == "sqlite" && filepath.Clean(previous.SQLiteDirectory) == filepath.Clean(target.SQLiteDirectory) && previous.RetentionDays != target.RetentionDays {
		defer m.mu.Unlock()
		if err := m.preserveHistoryCursors(ctx, previous, target); err != nil {
			return err
		}
		if err := m.switchRecording(target); err != nil {
			return err
		}
		return m.maintainSQLite(ctx, now)
	}
	m.mu.Unlock()
	if err := historyDirectory(target.ExportDirectory); err != nil {
		return err
	}
	resumePath := m.privatePath("history-migration.json")
	m.mu.Lock()
	owner := ""
	if job := m.jobs[m.busy]; job != nil {
		owner = job.owner
	}
	m.mu.Unlock()
	resume, _ := json.Marshal(map[string]any{"config": target, "owner": owner})
	if err := atomicFile(resumePath, resume); err != nil {
		return err
	}
	defer os.Remove(resumePath)
	next, err := openHistoryRepository(ctx, target)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			next.Close()
		}
	}()
	m.mu.Lock()
	m.migrating = true
	source := m.store
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.migrating = false; m.mu.Unlock() }()
	if err != nil {
		return err
	}
	rows, err := m.journal.db.QueryContext(ctx, "SELECT viewer FROM history_viewers WHERE (?='' OR viewer=?) ORDER BY viewer", m.owner, m.owner)
	if err != nil {
		return err
	}
	var viewers []string
	for rows.Next() {
		var viewer string
		if err = rows.Scan(&viewer); err != nil {
			break
		}
		viewers = append(viewers, viewer)
	}
	scanErr := rows.Err()
	rows.Close()
	if err == nil {
		err = scanErr
	}
	if err != nil {
		return err
	}
	var total, verified int64
	query := func(scope string, limit int) historyQuery {
		q := historyQuery{Scope: scope, Limit: limit}
		if target.Engine == "sqlite" {
			q.From = targetCutoff.Format(time.RFC3339Nano)
		}
		return q
	}
	for _, viewer := range viewers {
		for _, scope := range []string{"own", "others"} {
			page, e := source.Query(ctx, viewer, query(scope, 1))
			if e != nil {
				return e
			}
			total += page.Total
		}
	}
	for _, viewer := range viewers {
		for _, scope := range []string{"own", "others"} {
			q := query(scope, 50)
			for {
				page, e := source.Query(ctx, viewer, q)
				if e != nil {
					return e
				}
				for _, meta := range page.Items {
					record, e := source.Get(ctx, viewer, scope, historyText(meta["id"]))
					if e != nil {
						return e
					}
					if target.Engine == "sqlite" && expiredHistoryAtCutoff(record, targetCutoff) {
						continue
					}
					recoverHistoryUsage(record)
					if e = next.Save(ctx, viewer, scope, record); e != nil {
						return e
					}
					check, e := next.Get(ctx, viewer, scope, historyText(meta["id"]))
					if e != nil {
						return e
					}
					a, _ := json.Marshal(record)
					b, _ := json.Marshal(check)
					if sha256.Sum256(a) != sha256.Sum256(b) {
						return errors.New("history migration checksum mismatch")
					}
					verified++
					if total > 0 {
						m.progress(m.migration, int(min(verified, total)*95/total))
					}
				}
				if page.NextCursor == "" {
					break
				}
				q.Before = page.NextCursor
			}
			sourceCount, e := source.Query(ctx, viewer, query(scope, 1))
			if e != nil {
				return e
			}
			targetCount, e := next.Query(ctx, viewer, historyQuery{Scope: scope, Limit: 1})
			if e != nil {
				return e
			}
			if sourceCount.Total < targetCount.Total {
				return errors.New("history migration count mismatch")
			}
		}
		before := ""
		for {
			accounts, e := source.CandidateStates(ctx, viewer, before, 100)
			if e != nil {
				return e
			}
			for _, account := range accounts {
				if e = next.Candidate(ctx, viewer, historyText(account["id"]), historyText(account["email"]), account["active"] == true); e != nil {
					return e
				}
			}
			if len(accounts) < 100 {
				break
			}
			last := accounts[len(accounts)-1]
			before = historyCursor(historyText(last["email"]), historyText(last["id"]))
		}
	}
	var checkpoint int64
	for {
		m.mu.Lock()
		var upper int64
		if err = m.journal.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq),0) FROM history_pending WHERE (?='' OR viewer=?)", m.owner, m.owner).Scan(&upper); err != nil {
			m.mu.Unlock()
			return err
		}
		if upper <= checkpoint {
			break
		}
		m.mu.Unlock()
		if err = m.replayThrough(ctx, next, checkpoint, upper); err != nil {
			return err
		}
		checkpoint = upper
	}
	defer m.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	rows, err = m.journal.db.QueryContext(ctx, "SELECT viewer FROM history_viewers WHERE (?='' OR viewer=?) ORDER BY viewer", m.owner, m.owner)
	if err != nil {
		return err
	}
	viewers = nil
	for rows.Next() {
		var viewer string
		if err = rows.Scan(&viewer); err != nil {
			break
		}
		viewers = append(viewers, viewer)
	}
	scanErr = rows.Err()
	rows.Close()
	if err == nil {
		err = scanErr
	}
	if err != nil {
		return err
	}
	for _, viewer := range viewers {
		for _, scope := range []string{"own", "others"} {
			a, e := source.Query(ctx, viewer, query(scope, 1))
			if e != nil {
				return e
			}
			b, e := next.Query(ctx, viewer, historyQuery{Scope: scope, Limit: 1})
			if e != nil {
				return e
			}
			if a.Total != b.Total {
				return errors.New("history migration final count mismatch")
			}
		}
	}
	raw, _ := json.Marshal(target)
	if err = atomicFile(m.configPath(), raw); err != nil {
		return err
	}
	m.store = next
	m.config = target
	keep = true
	// Handles are closed only after the short cutover lock, after readers release that lock.
	source.Close()
	if _, err = m.journal.db.ExecContext(ctx, "DELETE FROM history_pending WHERE (?='' OR viewer=?)", m.owner, m.owner); err != nil {
		return err
	}
	if target.Engine == "sqlite" && target.RetentionDays != previous.RetentionDays {
		return m.maintainSQLite(ctx, now)
	}
	return nil
}
func (c *core) historyIdentity(session string) (string, error) {
	if session == "" || c.historySessionRevoked(session) {
		return "", errors.New("login required")
	}
	hash := sha256.Sum256([]byte(session))
	c.historyIdentityMu.Lock()
	if c.historyIdentityHash == hash && c.historyIdentityAccount != "" {
		account := c.historyIdentityAccount
		c.historyIdentityMu.Unlock()
		return account, nil
	}
	version := c.historyIdentityVersion
	c.historyIdentityMu.Unlock()
	var account struct {
		ID string `json:"id"`
	}
	if err := historyRemote(context.Background(), c.control, "/me", "Bearer "+session, nil, &account); err != nil {
		return "", err
	}
	if !nodePattern.MatchString(account.ID) {
		return "", errors.New("invalid history account")
	}
	c.historyIdentityMu.Lock()
	if version != c.historyIdentityVersion {
		c.historyIdentityMu.Unlock()
		return "", errors.New("history account changed during verification")
	}
	previous := c.historyIdentityHash
	changed := c.historyIdentityAccount != "" && previous != hash
	c.historyIdentityHash = hash
	c.historyIdentityAccount = account.ID
	c.historyIdentityMu.Unlock()
	if changed {
		c.historyEvents.revoke(previous)
		c.historyMu.Lock()
		if c.history != nil && c.history.manager != nil {
			m := c.history.manager
			m.mu.Lock()
			for _, job := range m.jobs {
				if job.State == "running" {
					job.cancel()
				}
			}
			m.mu.Unlock()
		}
		c.historyMu.Unlock()
	}
	return account.ID, nil
}

func (m *historyManager) restore(ctx context.Context) error {
	if m.config.Engine == "off" {
		return nil
	}
	var err error
	// The original database and usage outboxes remain authoritative recovery material.
	var imported string
	m.journal.db.QueryRow("SELECT value FROM history_meta WHERE key=?", "legacy-imported:"+m.owner).Scan(&imported)
	if imported != "1" {
		var rowid int64
		m.journal.db.QueryRow("SELECT CAST(value AS INTEGER) FROM history_meta WHERE key=?", "legacy-import-cursor:"+m.owner).Scan(&rowid)
		for {
			var next int64
			var account, raw string
			err = m.journal.db.QueryRowContext(ctx, "SELECT rowid,account,payload FROM exchanges WHERE rowid>? AND deleted_at IS NULL AND (?='' OR account=?) ORDER BY rowid LIMIT 1", rowid, m.owner, m.owner).Scan(&next, &account, &raw)
			if err == sql.ErrNoRows {
				err = nil
				break
			}
			if err != nil {
				break
			}
			value, decodeErr := m.journal.decodeBody(ctx, account, raw)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			if err = m.apply(ctx, account, "own", "record", historyText(value["id"]), value); err != nil {
				break
			}
			rowid = next
			if _, err = m.journal.db.ExecContext(ctx, "INSERT OR REPLACE INTO history_meta(key,value) VALUES(?,?)", "legacy-import-cursor:"+m.owner, rowid); err != nil {
				break
			}
		}
		if err != nil {
			m.store.Close()
			return err
		}
		if _, err = m.journal.db.Exec("INSERT OR REPLACE INTO history_meta(key,value) VALUES(?,'1')", "legacy-imported:"+m.owner); err != nil {
			m.store.Close()
			return err
		}
	}

	return m.drainPending(ctx)
}
