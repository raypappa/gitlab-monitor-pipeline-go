package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenUsageStoreCreatesDatabaseAndSchema(t *testing.T) {
	// Arrange
	databasePath := filepath.Join(t.TempDir(), "nested", "usage.sqlite")

	// Act
	store, actualPath, err := openUsageStore(context.Background(), databasePath, "https://gitlab.example")
	if err != nil {
		t.Fatalf("openUsageStore() error = %v", err)
	}
	defer func() { _ = store.close() }()

	// Assert
	if actualPath != databasePath {
		t.Fatalf("database path = %q, want %q", actualPath, databasePath)
	}
	info, err := os.Stat(databasePath)
	if err != nil {
		t.Fatalf("stat database: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("database permissions = %o, want 600", got)
	}
	var tableCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('projects', 'jobs', 'project_sync_state', 'legacy_cache_imports')`).Scan(&tableCount); err != nil {
		t.Fatalf("query schema: %v", err)
	}
	if tableCount != 4 {
		t.Fatalf("schema table count = %d, want 4", tableCount)
	}
}

func TestOpenUsageStoreMigratesRunnerMetadataColumns(t *testing.T) {
	// Arrange
	databasePath := filepath.Join(t.TempDir(), "usage.sqlite")
	legacyDB, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	_, err = legacyDB.Exec(`CREATE TABLE jobs (
host TEXT NOT NULL, project_id INTEGER NOT NULL, project_path TEXT NOT NULL,
job_id INTEGER NOT NULL, name TEXT NOT NULL, created_at TEXT NOT NULL,
duration REAL, status TEXT NOT NULL DEFAULT '', PRIMARY KEY(host, project_id, job_id))`)
	if err != nil {
		t.Fatalf("create legacy jobs table: %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	// Act
	store, _, err := openUsageStore(context.Background(), databasePath, "https://gitlab.example")
	if err != nil {
		t.Fatalf("openUsageStore() migration error = %v", err)
	}
	defer func() { _ = store.close() }()

	// Assert
	columns := make(map[string]bool)
	rows, err := store.db.Query(`PRAGMA table_info(jobs)`)
	if err != nil {
		t.Fatalf("inspect migrated jobs table: %v", err)
	}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan migrated schema: %v", err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close migrated schema rows: %v", err)
	}
	if !columns["runner_type"] || !columns["tag_list"] {
		t.Fatalf("migrated job columns = %#v, want runner_type and tag_list", columns)
	}
}

func TestImportLegacyUsageCacheIsIdempotent(t *testing.T) {
	// Arrange
	ctx := context.Background()
	store, _, err := openUsageStore(ctx, filepath.Join(t.TempDir(), "usage.sqlite"), "https://gitlab.example")
	if err != nil {
		t.Fatalf("openUsageStore() error = %v", err)
	}
	defer func() { _ = store.close() }()
	cacheDirectory := t.TempDir()
	project := groupProject{ID: 42, PathWithNamespace: "example-group/service"}
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	requestPath := initialJobsPath(project.ID)
	cachePath, err := usagePageCachePath(cacheDirectory, store.host, project.ID, requestPath)
	if err != nil {
		t.Fatalf("usagePageCachePath() error = %v", err)
	}
	job := usageJob{ID: 101, Name: "test", Duration: floatPointer(90), CreatedAt: "2026-10-02T03:04:05Z", Status: "success"}
	if err := writeUsagePageCache(cachePath, usagePageCache{
		Version:  usagePageCacheVersion,
		CachedAt: time.Now().UTC(),
		Jobs:     []usageJob{job},
	}); err != nil {
		t.Fatalf("write legacy cache: %v", err)
	}

	// Act
	firstPages, firstJobs, err := store.importLegacyCaches(ctx, cacheDirectory, []groupProject{project}, since)
	if err != nil {
		t.Fatalf("first import error = %v", err)
	}
	secondPages, secondJobs, err := store.importLegacyCaches(ctx, cacheDirectory, []groupProject{project}, since)
	if err != nil {
		t.Fatalf("second import error = %v", err)
	}

	// Assert
	if firstPages != 1 || firstJobs != 1 {
		t.Fatalf("first import = %d pages, %d jobs; want 1 page and 1 job", firstPages, firstJobs)
	}
	if secondPages != 0 || secondJobs != 0 {
		t.Fatalf("second import = %d pages, %d jobs; want no duplicate import", secondPages, secondJobs)
	}
	rows, err := store.queryUsageJobs(ctx, project, since, since.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("query imported jobs: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != job.ID || rows[0].Name != job.Name {
		t.Fatalf("imported jobs = %#v, want the single cached job", rows)
	}
	state, err := store.projectSyncState(ctx, project.ID)
	if err != nil {
		t.Fatalf("projectSyncState() error = %v", err)
	}
	if !state.hasCoverage || !state.coverageSince.Equal(since) {
		t.Fatalf("legacy sync coverage = %#v, want coverage from %s", state, since)
	}
}

func TestImportLegacyOffsetUsageCache(t *testing.T) {
	// Arrange
	ctx := context.Background()
	store, _, err := openUsageStore(ctx, filepath.Join(t.TempDir(), "usage.sqlite"), "https://gitlab.example")
	if err != nil {
		t.Fatalf("openUsageStore() error = %v", err)
	}
	defer func() { _ = store.close() }()
	cacheDirectory := t.TempDir()
	project := groupProject{ID: 73, PathWithNamespace: "example-group/legacy"}
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cachePath := legacyOffsetPageCachePath(cacheDirectory, store.host, project.ID, 1)
	if err := writeUsagePageCache(cachePath, usagePageCache{
		Version:  usagePageCacheVersion,
		CachedAt: time.Now().UTC(),
		Jobs:     []usageJob{{ID: 701, Name: "legacy-build", Duration: floatPointer(60), CreatedAt: "2026-10-02T00:00:00Z"}},
	}); err != nil {
		t.Fatalf("write offset cache: %v", err)
	}

	// Act
	pages, jobsImported, err := store.importLegacyCaches(ctx, cacheDirectory, []groupProject{project}, since)
	if err != nil {
		t.Fatalf("importLegacyCaches() error = %v", err)
	}

	// Assert
	if pages != 1 || jobsImported != 1 {
		t.Fatalf("offset cache import = %d pages, %d jobs; want 1 page and 1 job", pages, jobsImported)
	}
	jobs, err := store.queryUsageJobs(ctx, project, since, since.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("query imported jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Name != "legacy-build" {
		t.Fatalf("offset-imported jobs = %#v, want the legacy-build job", jobs)
	}
}

func TestSyncProjectPersistsHistoricalProgressAndJobs(t *testing.T) {
	// Arrange
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("pagination") != "keyset" || r.URL.Query().Get("order_by") != "id" {
			t.Errorf("job request query = %s, want keyset pagination ordered by ID", r.URL.RawQuery)
		}
		if r.URL.Query().Get("id_before") == "2" {
			writeJSON(t, w, []usageJob{{ID: 1, Name: "old", Duration: floatPointer(30), CreatedAt: "2026-09-30T23:59:59Z", Status: "success"}})
			return
		}
		next := *r.URL
		query := next.Query()
		query.Set("id_before", "2")
		next.RawQuery = query.Encode()
		w.Header().Set("Link", `<`+next.String()+`>; rel="next"`)
		writeJSON(t, w, []usageJob{{ID: 2, Name: "build", Duration: floatPointer(120), CreatedAt: "2026-10-02T12:00:00Z", Status: "success"}})
	}))
	defer server.Close()
	client := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	store, _, err := openUsageStore(context.Background(), filepath.Join(t.TempDir(), "usage.sqlite"), server.URL)
	if err != nil {
		t.Fatalf("openUsageStore() error = %v", err)
	}
	defer func() { _ = store.close() }()
	project := groupProject{ID: 7, PathWithNamespace: "example-group/project"}
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	until := since.Add(48 * time.Hour)
	options := usageOptions{cacheTTL: time.Minute, historicalCacheTTL: 24 * time.Hour}

	// Act
	stats, err := store.syncProject(context.Background(), client, project, since, until, options)
	if err != nil {
		t.Fatalf("syncProject() error = %v", err)
	}

	// Assert
	if requests != 2 || stats.pages != 2 || !stats.synced {
		t.Fatalf("sync result = %d requests, %#v; want two pages and completed sync", requests, stats)
	}
	state, err := store.projectSyncState(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("projectSyncState() error = %v", err)
	}
	if !state.hasCoverage || !state.coverageSince.Equal(since) || state.lastFullSync.IsZero() || state.resumePath != "" {
		t.Fatalf("completed project sync state = %#v, want complete coverage and cleared cursor", state)
	}
	jobs, err := store.queryUsageJobs(context.Background(), project, since, until)
	if err != nil {
		t.Fatalf("query synced jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != 2 || jobs[0].Name != "build" {
		t.Fatalf("synced usage jobs = %#v, want only the in-range build job", jobs)
	}
}

func TestSyncProjectUsesStoredJobsWhenHistoricalRefreshIsRateLimited(t *testing.T) {
	// Arrange
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		writeJSON(t, w, map[string]string{"message": "rate limited"})
	}))
	defer server.Close()
	ctx := context.Background()
	store, _, err := openUsageStore(ctx, filepath.Join(t.TempDir(), "usage.sqlite"), server.URL)
	if err != nil {
		t.Fatalf("openUsageStore() error = %v", err)
	}
	defer func() { _ = store.close() }()
	project := groupProject{ID: 8, PathWithNamespace: "example-group/cached"}
	since := time.Now().UTC().Add(-30 * 24 * time.Hour).Truncate(time.Second)
	now := time.Now().UTC()
	state := projectSyncState{
		coverageSince:  since,
		hasCoverage:    true,
		lastFullSync:   now.Add(-48 * time.Hour),
		lastRecentSync: now.Add(-48 * time.Hour),
	}
	if err := store.saveProjectSyncState(ctx, project.ID, state); err != nil {
		t.Fatalf("save existing sync state: %v", err)
	}
	if err := store.upsertJobs(ctx, project, []usageJob{{
		ID:        900,
		Name:      "cached-build",
		Duration:  floatPointer(180),
		CreatedAt: now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		Status:    "success",
	}}); err != nil {
		t.Fatalf("seed stored job: %v", err)
	}
	client := &client{baseURL: server.URL, token: "test-token", http: server.Client()}

	// Act
	stats, err := store.syncProject(ctx, client, project, since, now, usageOptions{
		cacheTTL:           time.Minute,
		historicalCacheTTL: 24 * time.Hour,
	})

	// Assert
	if err != nil {
		t.Fatalf("syncProject() error = %v", err)
	}
	if !stats.stale || !stats.readFromDatabase || stats.notice == "" {
		t.Fatalf("sync stats = %#v, want stale stored-data fallback", stats)
	}
	if requests != maxAPIRequestRetries+1 {
		t.Fatalf("rate-limit requests = %d, want %d attempts", requests, maxAPIRequestRetries+1)
	}
	jobs, err := store.queryUsageJobs(ctx, project, since, now)
	if err != nil {
		t.Fatalf("query stored fallback job: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Name != "cached-build" {
		t.Fatalf("stored fallback jobs = %#v, want cached-build", jobs)
	}
}
