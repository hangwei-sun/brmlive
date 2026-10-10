package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var analysisLocks = struct {
	sync.Mutex
	keys map[string]chan struct{}
}{keys: map[string]chan struct{}{}}
var analysisCacheName = regexp.MustCompile(`^[a-f0-9]{64}\.json$`)

// Only machine-generated immutable parts are shared. User edits and job files
// stay in the original owner-scoped directories and are never cached here.
func cachedNewsAnalysis(ctx context.Context, source string, duration float64, config newsSmartConfig,
	progress func(int), measure func(string, float64), run func() ([]newsPart, error)) ([]newsPart, error) {
	root, revision := os.Getenv("NEWS_ANALYSIS_CACHE_ROOT"), os.Getenv("NEWS_ANALYSIS_CACHE_REVISION")
	if root == "" || revision == "" {
		return run()
	}
	if st, err := os.Lstat(root); err == nil && st.Mode()&os.ModeSymlink != 0 {
		return run()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return run()
	}
	started := time.Now()
	file, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	digest := sha256.New()
	metadata, _ := json.Marshal([]any{revision, "news-boundary-v2", duration, config.ASRURL, config.ASRModel, config.ChatURL, config.ChatModel})
	_, _ = digest.Write(metadata)
	_, _ = digest.Write([]byte{0})
	buffer := make([]byte, 1024*1024)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		n, e := file.Read(buffer)
		if n > 0 {
			_, _ = digest.Write(buffer[:n])
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	after, err := os.Stat(source)
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return run()
	}
	measure("cache_hash_seconds", time.Since(started).Seconds())
	key := fmt.Sprintf("%x", digest.Sum(nil))
	lockKey := filepath.Join(root, key)
	for {
		analysisLocks.Lock()
		wait := analysisLocks.keys[lockKey]
		if wait == nil {
			analysisLocks.keys[lockKey] = make(chan struct{})
			analysisLocks.Unlock()
			break
		}
		analysisLocks.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		}
	}
	defer func() {
		analysisLocks.Lock()
		close(analysisLocks.keys[lockKey])
		delete(analysisLocks.keys, lockKey)
		analysisLocks.Unlock()
	}()
	path := filepath.Join(root, key+".json")
	if st, e := os.Lstat(path); e == nil && st.Mode().IsRegular() && st.Size() < 1024*1024 && time.Since(st.ModTime()) < 24*time.Hour {
		if data, e := os.ReadFile(path); e == nil {
			var parts []newsPart
			if json.Unmarshal(data, &parts) == nil && validAnalysisParts(parts, duration) {
				measure("analysis_cache_hit", 1)
				progress(99)
				return parts, nil
			}
		}
	}
	parts, err := run()
	if err != nil {
		return nil, err
	}
	after, err = os.Stat(source)
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || !validAnalysisParts(parts, duration) {
		return parts, nil
	}
	raw, err := json.Marshal(parts)
	if err != nil {
		return parts, nil
	}
	temp, err := os.CreateTemp(root, ".analysis-*.partial")
	if err != nil {
		return parts, nil
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err = temp.Write(raw); err != nil {
		temp.Close()
		return parts, nil
	}
	if err = temp.Close(); err != nil {
		return parts, nil
	}
	if os.Rename(name, path) != nil {
		return parts, nil
	}
	cleanAnalysisCache(root)
	return parts, nil
}

func validAnalysisParts(parts []newsPart, duration float64) bool {
	if len(parts) == 0 || len(parts) > 30 {
		return false
	}
	last := 0.0
	for _, p := range parts {
		if math.IsNaN(p.Start) || math.IsInf(p.Start, 0) || math.IsNaN(p.End) || math.IsInf(p.End, 0) || p.Start < last || p.End <= p.Start || p.End > duration+0.001 || strings.TrimSpace(p.Title) == "" || len([]rune(p.Title)) > 100 {
			return false
		}
		last = p.End
	}
	return true
}

func cleanAnalysisCache(root string) {
	entries, _ := os.ReadDir(root)
	type item struct {
		path     string
		modified time.Time
	}
	kept := []item{}
	for _, entry := range entries {
		if !analysisCacheName.MatchString(entry.Name()) {
			continue
		}
		st, e := entry.Info()
		if e != nil || !st.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if time.Since(st.ModTime()) > 24*time.Hour {
			_ = os.Remove(path)
		} else {
			kept = append(kept, item{path, st.ModTime()})
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].modified.Before(kept[j].modified) })
	for len(kept) > 128 {
		_ = os.Remove(kept[0].path)
		kept = kept[1:]
	}
}
