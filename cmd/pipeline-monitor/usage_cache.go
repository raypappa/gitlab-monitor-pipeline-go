package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const usagePageCacheVersion = 1

type usagePageCache struct {
	Version  int        `json:"version"`
	CachedAt time.Time  `json:"cached_at"`
	Jobs     []usageJob `json:"jobs"`
	NextPage string     `json:"next_page,omitempty"`
}

func usageCacheDirectory(configured string) (string, error) {
	if configured != "" {
		return filepath.Abs(configured)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache directory: %w", err)
	}
	return filepath.Join(base, "pipeline-monitor", "usage"), nil
}

func (c *client) usageJobPage(ctx context.Context, projectID int64, requestPath string, opts usageOptions, cacheDir string, cacheEnabled bool) ([]usageJob, string, bool, bool, string, error) {
	var staleEntry *usagePageCache
	cachePath := ""
	notice := ""
	if cacheEnabled {
		path, err := usagePageCachePath(cacheDir, c.baseURL, projectID, requestPath)
		if err != nil {
			notice = joinUsageNotices(notice, "could not resolve cache page path")
			cacheEnabled = false
		} else {
			cachePath = path
			if !opts.refresh {
				entry, err := readUsagePageCache(cachePath)
				if err == nil {
					age := time.Since(entry.CachedAt)
					if age >= 0 && age < usagePageCacheTTL(entry, opts, time.Now()) {
						return entry.Jobs, entry.NextPage, true, false, notice, nil
					}
					staleEntry = entry
				} else if !errors.Is(err, os.ErrNotExist) {
					notice = joinUsageNotices(notice, "one or more cache pages were unreadable")
				}
			}
		}
	}

	var jobs []usageJob
	headers, err := c.requestPage(ctx, http.MethodGet, requestPath, &jobs)
	if err != nil {
		if staleEntry != nil && !opts.refresh {
			notice = joinUsageNotices(notice, "using expired cached job pages after GitLab request errors")
			return staleEntry.Jobs, staleEntry.NextPage, true, true, notice, nil
		}
		return nil, "", false, false, notice, err
	}
	nextPath, _ := nextPage(requestPath, headers)
	if cacheEnabled {
		entry := usagePageCache{Version: usagePageCacheVersion, CachedAt: time.Now().UTC(), Jobs: jobs, NextPage: nextPath}
		if err := writeUsagePageCache(cachePath, entry); err != nil {
			notice = joinUsageNotices(notice, "could not write one or more usage cache pages")
		}
	}
	return jobs, nextPath, false, false, notice, nil
}

func usagePageCachePath(cacheDir, host string, projectID int64, requestPath string) (string, error) {
	if cacheDir == "" {
		return "", errors.New("cache directory is empty")
	}
	identity := fmt.Sprintf("%s\n%d\n%s\njob-pages-v2", host, projectID, requestPath)
	digest := sha256.Sum256([]byte(identity))
	return filepath.Join(cacheDir, hex.EncodeToString(digest[:])+".json"), nil
}

func usagePageCacheTTL(entry *usagePageCache, opts usageOptions, now time.Time) time.Duration {
	newest := time.Time{}
	for _, j := range entry.Jobs {
		createdAt, err := time.Parse(time.RFC3339Nano, j.CreatedAt)
		if err == nil && (newest.IsZero() || createdAt.After(newest)) {
			newest = createdAt
		}
	}
	if newest.IsZero() || newest.Before(now.Add(-usageRecentRefreshWindow)) {
		return opts.historicalCacheTTL
	}
	return opts.cacheTTL
}

func readUsagePageCache(path string) (*usagePageCache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entry usagePageCache
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	if entry.Version != usagePageCacheVersion {
		return nil, errors.New("unsupported usage cache version")
	}
	return &entry, nil
}

func writeUsagePageCache(path string, entry usagePageCache) error {
	return writeUsageCacheFile(path, entry)
}

func writeUsageCacheFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create cache directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".usage-*.tmp")
	if err != nil {
		return fmt.Errorf("create cache file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure cache file: %w", err)
	}
	if err := json.NewEncoder(temporary).Encode(value); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("encode cache file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("flush cache file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close cache file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace cache file: %w", err)
	}
	return nil
}
