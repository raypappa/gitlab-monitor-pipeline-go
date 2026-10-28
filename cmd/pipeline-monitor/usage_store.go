package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const databaseTimestampLayout = "2006-01-02T15:04:05.000000000Z"

type usageStore struct {
	db   *sql.DB
	host string
}

type projectSyncState struct {
	coverageSince  time.Time
	hasCoverage    bool
	lastFullSync   time.Time
	lastRecentSync time.Time
	resumeSince    time.Time
	resumePath     string
}

type usageSyncStats struct {
	pages            int
	synced           bool
	readFromDatabase bool
	stale            bool
	notice           string
}

func defaultUsageDatabasePath() (string, error) {
	cacheDirectory, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache directory: %w", err)
	}
	return filepath.Join(cacheDirectory, "pipeline-monitor", "usage.sqlite"), nil
}

func openUsageStore(ctx context.Context, configuredPath, host string) (*usageStore, string, error) {
	databasePath := configuredPath
	if databasePath == "" {
		var err error
		databasePath, err = defaultUsageDatabasePath()
		if err != nil {
			return nil, "", err
		}
	}
	databasePath, err := filepath.Abs(databasePath)
	if err != nil {
		return nil, "", fmt.Errorf("resolve SQLite path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o700); err != nil {
		return nil, "", fmt.Errorf("create SQLite directory: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.ToSlash(databasePath))
	if err != nil {
		return nil, "", fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("connect to SQLite database: %w", err)
	}
	store := &usageStore{db: db, host: host}
	if err := store.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, "", err
	}
	if err := os.Chmod(databasePath, 0o600); err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("secure SQLite database: %w", err)
	}
	return store, databasePath, nil
}

func (s *usageStore) close() error {
	return s.db.Close()
}

func (s *usageStore) initialize(ctx context.Context) error {
	for _, pragma := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure SQLite: %w", err)
		}
	}
	const schema = `
CREATE TABLE IF NOT EXISTS projects (
  host TEXT NOT NULL,
  project_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (host, project_id)
);
CREATE TABLE IF NOT EXISTS jobs (
  host TEXT NOT NULL,
  project_id INTEGER NOT NULL,
  project_path TEXT NOT NULL,
  job_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  duration REAL,
  status TEXT NOT NULL DEFAULT '',
  runner_type TEXT NOT NULL DEFAULT '',
  tag_list TEXT NOT NULL DEFAULT '[]',
  PRIMARY KEY (host, project_id, job_id)
);
CREATE INDEX IF NOT EXISTS jobs_by_project_date ON jobs(host, project_id, created_at);
CREATE INDEX IF NOT EXISTS jobs_by_project_name_date ON jobs(host, project_id, name, created_at);
CREATE TABLE IF NOT EXISTS project_sync_state (
  host TEXT NOT NULL,
  project_id INTEGER NOT NULL,
  coverage_since TEXT NOT NULL DEFAULT '',
  last_full_sync TEXT NOT NULL DEFAULT '',
  last_recent_sync TEXT NOT NULL DEFAULT '',
  resume_since TEXT NOT NULL DEFAULT '',
  resume_path TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (host, project_id)
);
CREATE TABLE IF NOT EXISTS legacy_cache_imports (
  cache_path TEXT PRIMARY KEY,
  modified_ns INTEGER NOT NULL,
  file_size INTEGER NOT NULL,
  imported_at TEXT NOT NULL
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize SQLite schema: %w", err)
	}
	if err := ensureUsageJobColumn(ctx, s.db, "runner_type", `TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := ensureUsageJobColumn(ctx, s.db, "tag_list", `TEXT NOT NULL DEFAULT '[]'`); err != nil {
		return err
	}
	return nil
}

func ensureUsageJobColumn(ctx context.Context, db *sql.DB, column, definition string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(jobs)")
	if err != nil {
		return fmt.Errorf("inspect jobs schema: %w", err)
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read jobs schema: %w", err)
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate jobs schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close jobs schema: %w", err)
	}
	if found {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE jobs ADD COLUMN "+column+" "+definition); err != nil {
		return fmt.Errorf("migrate jobs schema with %s: %w", column, err)
	}
	return nil
}

func (s *usageStore) upsertProjects(ctx context.Context, projects []groupProject) error {
	const query = `INSERT INTO projects(host, project_id, path, archived) VALUES(?, ?, ?, ?)
ON CONFLICT(host, project_id) DO UPDATE SET path = excluded.path, archived = excluded.archived`
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin project transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statement, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare project update: %w", err)
	}
	defer func() { _ = statement.Close() }()
	for _, project := range projects {
		archived := 0
		if project.Archived {
			archived = 1
		}
		if _, err := statement.ExecContext(ctx, s.host, project.ID, project.PathWithNamespace, archived); err != nil {
			return fmt.Errorf("store project %s: %w", project.PathWithNamespace, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project transaction: %w", err)
	}
	return nil
}

func (s *usageStore) importLegacyCaches(ctx context.Context, cacheDirectory string, projects []groupProject, since time.Time) (int, int, error) {
	if cacheDirectory == "" {
		return 0, 0, nil
	}
	entries, err := os.ReadDir(cacheDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read legacy cache directory: %w", err)
	}
	cacheFiles := make(map[string]bool, len(entries))
	jsonFileCount := 0
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			cacheFiles[entry.Name()] = true
			jsonFileCount++
		}
	}
	if jsonFileCount == 0 {
		return 0, 0, nil
	}
	pagesImported := 0
	jobsImported := 0
	for _, project := range projects {
		projectPages, projectJobs, legacyCoverage, fullCachedAt, recentCachedAt, err := s.importLegacyOffsetPages(ctx, cacheDirectory, cacheFiles, jsonFileCount, project, since)
		if err != nil {
			return pagesImported, jobsImported, err
		}
		pagesImported += projectPages
		jobsImported += projectJobs
		keysetPages, keysetJobs, keysetCoverage, keysetFullAt, keysetRecentAt, err := s.importLegacyKeysetPages(ctx, cacheDirectory, cacheFiles, jsonFileCount, project, since)
		if err != nil {
			return pagesImported, jobsImported, err
		}
		pagesImported += keysetPages
		jobsImported += keysetJobs
		if keysetCoverage {
			legacyCoverage = true
			fullCachedAt = earlierNonzero(fullCachedAt, keysetFullAt)
			recentCachedAt = laterNonzero(recentCachedAt, keysetRecentAt)
		}
		if legacyCoverage {
			if err := s.markLegacyCoverage(ctx, project.ID, since, fullCachedAt, recentCachedAt); err != nil {
				return pagesImported, jobsImported, err
			}
		}
	}
	return pagesImported, jobsImported, nil
}

func (s *usageStore) importLegacyOffsetPages(ctx context.Context, cacheDirectory string, cacheFiles map[string]bool, maxPage int, project groupProject, since time.Time) (int, int, bool, time.Time, time.Time, error) {
	pagesImported := 0
	jobsImported := 0
	nextContiguousPage := 1
	contiguous := true
	complete := false
	var fullCachedAt, recentCachedAt time.Time
	for page := 1; page <= maxPage; page++ {
		path := ""
		for _, candidate := range legacyOffsetPageCachePaths(cacheDirectory, s.host, project.ID, page) {
			if cacheFiles[filepath.Base(candidate)] {
				path = candidate
				break
			}
		}
		if path == "" {
			if contiguous && page == nextContiguousPage {
				contiguous = false
			}
			continue
		}
		entry, err := readUsagePageCache(path)
		if err != nil {
			continue
		}
		imported, err := s.importLegacyPageFile(ctx, path, project, entry)
		if err != nil {
			return pagesImported, jobsImported, false, time.Time{}, time.Time{}, err
		}
		if imported {
			pagesImported++
			jobsImported += len(entry.Jobs)
		}
		if page == 1 {
			recentCachedAt = entry.CachedAt
		}
		if contiguous && page == nextContiguousPage {
			nextContiguousPage++
			fullCachedAt = earlierNonzero(fullCachedAt, entry.CachedAt)
			oldest, hasTimestamp := oldestJobTime(entry.Jobs)
			if len(entry.Jobs) < usagePageSize || (hasTimestamp && oldest.Before(since)) {
				complete = true
			}
		} else if page > nextContiguousPage {
			contiguous = false
		}
	}
	if !complete {
		return pagesImported, jobsImported, false, time.Time{}, recentCachedAt, nil
	}
	return pagesImported, jobsImported, true, fullCachedAt, recentCachedAt, nil
}

func (s *usageStore) importLegacyKeysetPages(ctx context.Context, cacheDirectory string, cacheFiles map[string]bool, maxPages int, project groupProject, since time.Time) (int, int, bool, time.Time, time.Time, error) {
	requestPath := initialJobsPath(project.ID)
	pagesImported := 0
	jobsImported := 0
	var fullCachedAt, recentCachedAt time.Time
	for page := 0; page <= maxPages; page++ {
		path, err := usagePageCachePath(cacheDirectory, s.host, project.ID, requestPath)
		if err != nil {
			return pagesImported, jobsImported, false, time.Time{}, time.Time{}, err
		}
		if !cacheFiles[filepath.Base(path)] {
			return pagesImported, jobsImported, false, time.Time{}, time.Time{}, nil
		}
		entry, err := readUsagePageCache(path)
		if err != nil {
			return pagesImported, jobsImported, false, time.Time{}, time.Time{}, nil
		}
		imported, err := s.importLegacyPageFile(ctx, path, project, entry)
		if err != nil {
			return pagesImported, jobsImported, false, time.Time{}, time.Time{}, err
		}
		if imported {
			pagesImported++
			jobsImported += len(entry.Jobs)
		}
		if page == 0 {
			recentCachedAt = entry.CachedAt
		}
		fullCachedAt = earlierNonzero(fullCachedAt, entry.CachedAt)
		oldest, hasTimestamp := oldestJobTime(entry.Jobs)
		if len(entry.Jobs) == 0 || (hasTimestamp && oldest.Before(since)) || entry.NextPage == "" {
			return pagesImported, jobsImported, true, fullCachedAt, recentCachedAt, nil
		}
		requestPath = entry.NextPage
	}
	return pagesImported, jobsImported, false, time.Time{}, recentCachedAt, nil
}

func (s *usageStore) importLegacyPageFile(ctx context.Context, cachePath string, project groupProject, entry *usagePageCache) (bool, error) {
	absolutePath, err := filepath.Abs(cachePath)
	if err != nil {
		return false, err
	}
	fileInfo, err := os.Stat(absolutePath)
	if err != nil {
		return false, err
	}
	var modifiedNS, fileSize int64
	err = s.db.QueryRowContext(ctx, `SELECT modified_ns, file_size FROM legacy_cache_imports WHERE cache_path=?`, absolutePath).Scan(&modifiedNS, &fileSize)
	if err == nil && modifiedNS == fileInfo.ModTime().UnixNano() && fileSize == fileInfo.Size() {
		return false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check legacy cache import: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin legacy cache transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.upsertJobsTx(ctx, tx, project, entry.Jobs); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO legacy_cache_imports(cache_path, modified_ns, file_size, imported_at)
VALUES(?, ?, ?, ?) ON CONFLICT(cache_path) DO UPDATE SET modified_ns=excluded.modified_ns, file_size=excluded.file_size, imported_at=excluded.imported_at`, absolutePath, fileInfo.ModTime().UnixNano(), fileInfo.Size(), formatDatabaseTime(time.Now()))
	if err != nil {
		return false, fmt.Errorf("record legacy cache import: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit legacy cache import: %w", err)
	}
	return true, nil
}

func legacyOffsetPageCachePath(cacheDirectory, host string, projectID int64, pageNumber int) string {
	return legacyOffsetPageCachePaths(cacheDirectory, host, projectID, pageNumber)[0]
}

func legacyOffsetPageCachePaths(cacheDirectory, host string, projectID int64, pageNumber int) []string {
	requestPaths := []string{
		fmt.Sprintf("/api/v4/projects/%d/jobs?page=%d&per_page=%d", projectID, pageNumber, usagePageSize),
		fmt.Sprintf("/api/v4/projects/%d/jobs?per_page=%d&page=%d", projectID, usagePageSize, pageNumber),
	}
	identities := []string{
		fmt.Sprintf("%s\n%d\n%d\n%d\njob-pages-v1", host, projectID, pageNumber, usagePageSize),
		fmt.Sprintf("%s\n%d\n%d\njob-pages-v1", host, projectID, pageNumber),
	}
	for _, requestPath := range requestPaths {
		identities = append(identities,
			fmt.Sprintf("%s\n%d\n%s\njob-pages-v1", host, projectID, requestPath),
			fmt.Sprintf("%s\n%d\n%s", host, projectID, requestPath),
		)
	}
	paths := make([]string, 0, len(identities))
	seen := make(map[string]bool, len(identities))
	for _, identity := range identities {
		digest := sha256.Sum256([]byte(identity))
		path := filepath.Join(cacheDirectory, hex.EncodeToString(digest[:])+".json")
		if !seen[path] {
			paths = append(paths, path)
			seen[path] = true
		}
	}
	return paths
}

func (s *usageStore) markLegacyCoverage(ctx context.Context, projectID int64, since, fullCachedAt, recentCachedAt time.Time) error {
	state, err := s.projectSyncState(ctx, projectID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if fullCachedAt.After(now) {
		fullCachedAt = time.Time{}
	}
	if recentCachedAt.After(now) {
		recentCachedAt = time.Time{}
	}
	if !state.hasCoverage || since.Before(state.coverageSince) {
		state.coverageSince = since
		state.hasCoverage = true
	}
	if !fullCachedAt.IsZero() && (state.lastFullSync.IsZero() || fullCachedAt.Before(state.lastFullSync)) {
		state.lastFullSync = fullCachedAt
	}
	if recentCachedAt.After(state.lastRecentSync) {
		state.lastRecentSync = recentCachedAt
	}
	return s.saveProjectSyncState(ctx, projectID, state)
}

func (s *usageStore) saveProjectSyncState(ctx context.Context, projectID int64, state projectSyncState) error {
	coverageSince := ""
	if state.hasCoverage {
		coverageSince = formatDatabaseTime(state.coverageSince)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO project_sync_state(host, project_id, coverage_since, last_full_sync, last_recent_sync, resume_since, resume_path)
VALUES(?, ?, ?, ?, ?, ?, ?) ON CONFLICT(host, project_id) DO UPDATE SET
coverage_since=excluded.coverage_since,
last_full_sync=excluded.last_full_sync,
last_recent_sync=excluded.last_recent_sync,
resume_since=excluded.resume_since,
resume_path=excluded.resume_path`, s.host, projectID, coverageSince, optionalDatabaseTime(state.lastFullSync), optionalDatabaseTime(state.lastRecentSync), optionalDatabaseTime(state.resumeSince), state.resumePath)
	if err != nil {
		return fmt.Errorf("save project sync state: %w", err)
	}
	return nil
}

func optionalDatabaseTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return formatDatabaseTime(value)
}

func parseDatabaseTime(value string) (time.Time, error) {
	return time.Parse(databaseTimestampLayout, value)
}

func earlierNonzero(left, right time.Time) time.Time {
	if left.IsZero() || (!right.IsZero() && right.Before(left)) {
		return right
	}
	return left
}

func laterNonzero(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func (s *usageStore) syncProject(ctx context.Context, c *client, project groupProject, since, until time.Time, opts usageOptions) (usageSyncStats, error) {
	state, err := s.projectSyncState(ctx, project.ID)
	if err != nil {
		return usageSyncStats{}, err
	}
	now := time.Now().UTC()
	coverageIsSufficient := state.hasCoverage && !since.Before(state.coverageSince)
	fullSyncExpired := state.lastFullSync.IsZero() || state.lastFullSync.After(now) || opts.historicalCacheTTL == 0 || now.Sub(state.lastFullSync) >= opts.historicalCacheTTL
	resumePartial := !opts.noCache && !opts.refresh && state.resumeSince.Equal(since) && state.resumePath != ""
	needsFullSync := opts.noCache || opts.refresh || !coverageIsSufficient || fullSyncExpired
	if resumePartial {
		stats := usageSyncStats{readFromDatabase: true}
		lastSync := latestTime(state.lastRecentSync, state.lastFullSync)
		if shouldSyncRecent(until, lastSync, now, opts.cacheTTL) {
			recentSince := recentSyncSince(since, lastSync, now)
			recentPages, err := s.syncRecentJobs(ctx, c, project, recentSince)
			stats.pages += recentPages
			if err != nil {
				return stats, fmt.Errorf("refresh recent jobs before resuming historical sync: %w", err)
			}
			if err := s.setLastRecentSync(ctx, project.ID, now); err != nil {
				return stats, err
			}
		}
		fullPages, err := s.syncFullJobs(ctx, c, project, since, state.resumePath)
		stats.pages += fullPages
		stats.synced = fullPages > 0
		if err != nil {
			return stats, fmt.Errorf("resume historical job sync: %w", err)
		}
		return stats, nil
	}
	if needsFullSync {
		stats := usageSyncStats{readFromDatabase: state.hasCoverage}
		fullPages, err := s.syncFullJobs(ctx, c, project, since, "")
		stats.pages = fullPages
		stats.synced = fullPages > 0
		if err != nil {
			if coverageIsSufficient && !opts.noCache && !opts.refresh {
				stats.stale = true
				stats.notice = "using stored job data after GitLab could not refresh historical usage"
				return stats, nil
			}
			return stats, fmt.Errorf("full job sync: %w", err)
		}
		return stats, nil
	}

	stats := usageSyncStats{readFromDatabase: true}
	lastSync := latestTime(state.lastRecentSync, state.lastFullSync)
	if shouldSyncRecent(until, lastSync, now, opts.cacheTTL) {
		recentSince := recentSyncSince(since, lastSync, now)
		pages, err := s.syncRecentJobs(ctx, c, project, recentSince)
		stats.pages = pages
		stats.synced = pages > 0
		if err != nil {
			stats.stale = true
			stats.notice = "using stored job data after GitLab could not refresh recent usage"
			return stats, nil
		}
		if err := s.setLastRecentSync(ctx, project.ID, now); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *usageStore) projectSyncState(ctx context.Context, projectID int64) (projectSyncState, error) {
	const query = `SELECT coverage_since, last_full_sync, last_recent_sync, resume_since, resume_path
FROM project_sync_state WHERE host = ? AND project_id = ?`
	var state projectSyncState
	var coverageSince, lastFullSync, lastRecentSync, resumeSince string
	err := s.db.QueryRowContext(ctx, query, s.host, projectID).Scan(&coverageSince, &lastFullSync, &lastRecentSync, &resumeSince, &state.resumePath)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("read project sync state: %w", err)
	}
	if coverageSince != "" {
		state.coverageSince, err = parseDatabaseTime(coverageSince)
		if err != nil {
			return state, fmt.Errorf("parse project coverage date: %w", err)
		}
		state.hasCoverage = true
	}
	if lastFullSync != "" {
		state.lastFullSync, err = parseDatabaseTime(lastFullSync)
		if err != nil {
			return state, fmt.Errorf("parse project full-sync date: %w", err)
		}
	}
	if lastRecentSync != "" {
		state.lastRecentSync, err = parseDatabaseTime(lastRecentSync)
		if err != nil {
			return state, fmt.Errorf("parse project recent-sync date: %w", err)
		}
	}
	if resumeSince != "" {
		state.resumeSince, err = parseDatabaseTime(resumeSince)
		if err != nil {
			return state, fmt.Errorf("parse project resume date: %w", err)
		}
	}
	return state, nil
}

func (s *usageStore) syncFullJobs(ctx context.Context, c *client, project groupProject, since time.Time, startPath string) (int, error) {
	if startPath == "" {
		startPath = initialJobsPath(project.ID)
	}
	if err := s.setResume(ctx, project.ID, since, startPath); err != nil {
		return 0, err
	}
	path := startPath
	pages := 0
	for path != "" {
		jobs, nextPath, err := c.fetchUsageJobPage(ctx, path)
		if err != nil {
			return pages, err
		}
		pages++
		oldest, hasTimestamp := oldestJobTime(jobs)
		complete := len(jobs) == 0 || nextPath == "" || (hasTimestamp && oldest.Before(since))
		if err := s.saveSyncPage(ctx, project, since, jobs, nextPath, complete); err != nil {
			return pages, err
		}
		if complete {
			return pages, nil
		}
		path = nextPath
	}
	return pages, nil
}

func (s *usageStore) syncRecentJobs(ctx context.Context, c *client, project groupProject, since time.Time) (int, error) {
	path := initialJobsPath(project.ID)
	pages := 0
	for path != "" {
		jobs, nextPath, err := c.fetchUsageJobPage(ctx, path)
		if err != nil {
			return pages, err
		}
		pages++
		if err := s.upsertJobs(ctx, project, jobs); err != nil {
			return pages, err
		}
		oldest, hasTimestamp := oldestJobTime(jobs)
		if len(jobs) == 0 || nextPath == "" || (hasTimestamp && oldest.Before(since)) {
			return pages, nil
		}
		path = nextPath
	}
	return pages, nil
}

func (s *usageStore) saveSyncPage(ctx context.Context, project groupProject, since time.Time, jobs []usageJob, nextPath string, complete bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job page transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.upsertJobsTx(ctx, tx, project, jobs); err != nil {
		return err
	}
	if complete {
		if err := markFullSyncCompleteTx(ctx, tx, s.host, project.ID, since, time.Now().UTC()); err != nil {
			return err
		}
	} else if err := setSyncResumeTx(ctx, tx, s.host, project.ID, since, nextPath); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job page transaction: %w", err)
	}
	return nil
}

func (s *usageStore) upsertJobs(ctx context.Context, project groupProject, jobs []usageJob) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.upsertJobsTx(ctx, tx, project, jobs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job transaction: %w", err)
	}
	return nil
}

func (s *usageStore) upsertJobsTx(ctx context.Context, tx *sql.Tx, project groupProject, jobs []usageJob) error {
	const query = `INSERT INTO jobs(host, project_id, project_path, job_id, name, created_at, duration, status, runner_type, tag_list)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, project_id, job_id) DO UPDATE SET
  project_path = excluded.project_path,
  name = excluded.name,
  created_at = excluded.created_at,
  duration = excluded.duration,
  status = excluded.status,
  runner_type = excluded.runner_type,
  tag_list = excluded.tag_list`
	statement, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare job insert: %w", err)
	}
	defer func() { _ = statement.Close() }()
	for _, job := range jobs {
		if job.ID == 0 || job.CreatedAt == "" {
			continue
		}
		normalized, err := normalizeDatabaseJob(job)
		if err != nil {
			continue
		}
		tagList, err := json.Marshal(normalized.Tags)
		if err != nil {
			return fmt.Errorf("encode job %d runner tags: %w", normalized.ID, err)
		}
		runnerType := ""
		if normalized.Runner != nil {
			runnerType = normalized.Runner.RunnerType
		}
		if _, err := statement.ExecContext(ctx, s.host, project.ID, project.PathWithNamespace, normalized.ID, normalized.Name, normalized.CreatedAt, nullableDuration(normalized.Duration), normalized.Status, runnerType, string(tagList)); err != nil {
			return fmt.Errorf("store job %d in %s: %w", normalized.ID, project.PathWithNamespace, err)
		}
	}
	return nil
}

func (s *usageStore) setResume(ctx context.Context, projectID int64, since time.Time, path string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO project_sync_state(host, project_id, resume_since, resume_path)
VALUES(?, ?, ?, ?) ON CONFLICT(host, project_id) DO UPDATE SET resume_since=excluded.resume_since, resume_path=excluded.resume_path`, s.host, projectID, formatDatabaseTime(since), path)
	if err != nil {
		return fmt.Errorf("save project sync cursor: %w", err)
	}
	return nil
}

func setSyncResumeTx(ctx context.Context, tx *sql.Tx, host string, projectID int64, since time.Time, path string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_sync_state(host, project_id, resume_since, resume_path)
VALUES(?, ?, ?, ?) ON CONFLICT(host, project_id) DO UPDATE SET resume_since=excluded.resume_since, resume_path=excluded.resume_path`, host, projectID, formatDatabaseTime(since), path)
	if err != nil {
		return fmt.Errorf("save project sync cursor: %w", err)
	}
	return nil
}

func markFullSyncCompleteTx(ctx context.Context, tx *sql.Tx, host string, projectID int64, since, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_sync_state(host, project_id, coverage_since, last_full_sync, last_recent_sync)
VALUES(?, ?, ?, ?, ?) ON CONFLICT(host, project_id) DO UPDATE SET
  coverage_since = CASE WHEN project_sync_state.coverage_since = '' OR project_sync_state.coverage_since > excluded.coverage_since THEN excluded.coverage_since ELSE project_sync_state.coverage_since END,
  last_full_sync = excluded.last_full_sync,
  last_recent_sync = excluded.last_recent_sync,
  resume_since = '',
  resume_path = ''`, host, projectID, formatDatabaseTime(since), formatDatabaseTime(now), formatDatabaseTime(now))
	if err != nil {
		return fmt.Errorf("mark historical sync complete: %w", err)
	}
	return nil
}

func (s *usageStore) setLastRecentSync(ctx context.Context, projectID int64, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO project_sync_state(host, project_id, last_recent_sync)
VALUES(?, ?, ?) ON CONFLICT(host, project_id) DO UPDATE SET last_recent_sync=excluded.last_recent_sync`, s.host, projectID, formatDatabaseTime(at))
	if err != nil {
		return fmt.Errorf("save recent sync time: %w", err)
	}
	return nil
}

func (s *usageStore) queryUsageJobs(ctx context.Context, project groupProject, since, until time.Time) ([]usageJob, error) {
	const query = `SELECT job_id, name, created_at, duration, status, runner_type, tag_list FROM jobs
WHERE host=? AND project_id=? AND created_at>=? AND created_at<=? AND duration IS NOT NULL`
	rows, err := s.db.QueryContext(ctx, query, s.host, project.ID, formatDatabaseTime(since), formatDatabaseTime(until))
	if err != nil {
		return nil, fmt.Errorf("query cached jobs for %s: %w", project.PathWithNamespace, err)
	}
	defer func() { _ = rows.Close() }()
	var jobs []usageJob
	for rows.Next() {
		var job usageJob
		var duration sql.NullFloat64
		var runnerType, tagList string
		if err := rows.Scan(&job.ID, &job.Name, &job.CreatedAt, &duration, &job.Status, &runnerType, &tagList); err != nil {
			return nil, fmt.Errorf("read cached job for %s: %w", project.PathWithNamespace, err)
		}
		if duration.Valid {
			job.Duration = &duration.Float64
		}
		if runnerType != "" {
			job.Runner = &usageRunner{RunnerType: runnerType}
		}
		if err := json.Unmarshal([]byte(tagList), &job.Tags); err != nil {
			return nil, fmt.Errorf("decode cached runner tags for %s: %w", project.PathWithNamespace, err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cached jobs for %s: %w", project.PathWithNamespace, err)
	}
	return jobs, nil
}

func initialJobsPath(projectID int64) string {
	return fmt.Sprintf("/api/v4/projects/%d/jobs?pagination=keyset&per_page=%d&order_by=id&sort=desc", projectID, usagePageSize)
}

func (c *client) fetchUsageJobPage(ctx context.Context, path string) ([]usageJob, string, error) {
	var jobs []usageJob
	headers, err := c.requestPage(ctx, http.MethodGet, path, &jobs)
	if err != nil {
		return nil, "", err
	}
	next, _ := nextPage(path, headers)
	return jobs, next, nil
}

func oldestJobTime(jobs []usageJob) (time.Time, bool) {
	oldest := time.Time{}
	for _, job := range jobs {
		createdAt, err := time.Parse(time.RFC3339Nano, job.CreatedAt)
		if err != nil {
			continue
		}
		if oldest.IsZero() || createdAt.Before(oldest) {
			oldest = createdAt
		}
	}
	return oldest, !oldest.IsZero()
}

func latestTime(times ...time.Time) time.Time {
	var latest time.Time
	for _, value := range times {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}

func shouldSyncRecent(until, lastSync, now time.Time, cacheTTL time.Duration) bool {
	if !until.After(lastSync) {
		return false
	}
	return lastSync.IsZero() || cacheTTL == 0 || now.Sub(lastSync) >= cacheTTL
}

func recentSyncSince(since, lastSync, now time.Time) time.Time {
	cutoff := now.Add(-usageRecentRefreshWindow)
	if !lastSync.IsZero() {
		gapStart := lastSync.Add(-usageRecentRefreshWindow)
		if gapStart.Before(cutoff) {
			cutoff = gapStart
		}
	}
	if cutoff.Before(since) {
		return since
	}
	return cutoff
}

func formatDatabaseTime(value time.Time) string {
	return value.UTC().Format(databaseTimestampLayout)
}

func normalizeDatabaseJob(job usageJob) (usageJob, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, job.CreatedAt)
	if err != nil {
		return usageJob{}, fmt.Errorf("parse job creation time: %w", err)
	}
	job.CreatedAt = formatDatabaseTime(createdAt)
	return job, nil
}

func nullableDuration(duration *float64) any {
	if duration == nil || math.IsNaN(*duration) || math.IsInf(*duration, 0) {
		return nil
	}
	return *duration
}
