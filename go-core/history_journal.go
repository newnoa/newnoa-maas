package main

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Keep the durable usage/capture journal independent from the selectable history database.
func (c *core) safeHistoryJournalFile(path string) error {
	directory := defaultHistoryDatabase(c.home).SQLiteDirectory
	if filepath.Dir(path) != directory {
		return errors.New("invalid history journal directory")
	}
	name := filepath.Base(path)
	if name != "chat-history.sqlite" && name != "chat-history.sqlite-wal" && name != "chat-history.sqlite-shm" {
		return errors.New("invalid history journal file")
	}
	if !dockerRuntime() {
		return c.safeConfigFile(path)
	}
	for parent := directory; ; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return errors.New("unsafe history journal parent")
		}
		if parent == filepath.Dir(parent) {
			break
		}
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe history journal file")
	}
	return nil
}
func (c *core) openManagedHistoryJournal() (*chatHistory, error) {
	directory := defaultHistoryDatabase(c.home).SQLiteDirectory
	if err := historyDirectory(directory); err != nil {
		return nil, err
	}
	target := filepath.Join(directory, "chat-history.sqlite")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := c.safeHistoryJournalFile(target + suffix); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(target); err == nil {
		return openChatHistoryAt(directory)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	source := filepath.Join(c.home, ".newnoa", "chat-history.sqlite")
	if _, err := os.Stat(source); os.IsNotExist(err) {
		return openChatHistoryAt(directory)
	} else if err != nil {
		return nil, err
	}
	old, err := openChatHistory(c.home)
	if err != nil {
		return nil, err
	}
	defer old.db.Close()
	temporary, err := os.CreateTemp(directory, ".newnoa-journal-*.sqlite")
	if err != nil {
		return nil, err
	}
	name := temporary.Name()
	temporary.Close()
	os.Remove(name)
	defer os.Remove(name)
	// VACUUM INTO includes committed WAL pages while the source remains intact.
	if _, err = old.db.Exec("VACUUM INTO '" + strings.ReplaceAll(name, "'", "''") + "'"); err != nil {
		return nil, err
	}
	if err = os.Chmod(name, 0600); err != nil {
		return nil, err
	}
	check, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, err
	}
	var integrity string
	err = check.QueryRow("PRAGMA integrity_check").Scan(&integrity)
	if err == nil && integrity != "ok" {
		err = errors.New("history journal integrity verification failed")
	}
	for _, table := range []string{"exchanges", "hosted_usage_outbox", "private_usage_outbox", "history_settings", "history_consumption_pending"} {
		if err != nil {
			break
		}
		var sourceCount, targetCount int64
		err = old.db.QueryRow("SELECT count(*) FROM " + table).Scan(&sourceCount)
		if err == nil {
			err = check.QueryRow("SELECT count(*) FROM " + table).Scan(&targetCount)
		}
		if err == nil && sourceCount != targetCount {
			err = errors.New("history journal copy count mismatch")
		}
	}
	check.Close()
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(target); err == nil {
		return nil, errors.New("history journal destination already exists")
	}
	// Newer journals can already contain compressed body references in the sibling database.
	if err = migrateJournalBodyDatabase(old, directory); err != nil {
		return nil, err
	}
	if err = os.Rename(name, target); err != nil {
		return nil, err
	}
	return openChatHistoryAt(directory)
}
