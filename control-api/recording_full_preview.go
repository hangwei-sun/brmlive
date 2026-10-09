package main

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Called inside the single shared preview worker. Never touches recordings or
// employee exports. Recent caches are protected from eviction during playback.
func prepareFullPreview(root string, sourceBytes int64) error {
	const quota int64 = 32 * 1024 * 1024 * 1024
	const headroom uint64 = 4 * 1024 * 1024 * 1024
	if sourceBytes <= 0 || sourceBytes*2 > quota {
		return errors.New("preview too large")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	allowed := regexp.MustCompile(`^v[1-5]-[0-9]+\.mp4$`)
	type item struct {
		path     string
		size     int64
		modified time.Time
	}
	var files []item
	var used int64
	for _, entry := range entries {
		if entry.IsDir() || !allowed.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if time.Since(info.ModTime()) > 24*time.Hour {
			if os.Remove(path) == nil {
				continue
			}
		}
		used += info.Size()
		files = append(files, item{path, info.Size(), info.ModTime()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modified.Before(files[j].modified) })
	for _, file := range files {
		if used+sourceBytes*2 <= quota {
			break
		}
		if time.Since(file.modified) < 30*time.Minute {
			continue
		}
		if os.Remove(file.path) == nil {
			used -= file.size
		}
	}
	_, _, free, err := recordingFilesystemSpace(root)
	if err != nil || used+sourceBytes*2 > quota || free < headroom+uint64(sourceBytes)*2 {
		return errors.New("preview cache full")
	}
	return nil
}
