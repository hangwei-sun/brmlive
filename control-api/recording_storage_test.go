package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordingStorageReportsConfiguredFilesystem(t *testing.T) {
	root := t.TempDir()
	a := &app{recordingsRoot: root}
	w := httptest.NewRecorder()
	a.recordingStorage(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/storage", nil))
	var space recordingDiskSpace
	if err := json.Unmarshal(w.Body.Bytes(), &space); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || !space.Available || space.TotalBytes == 0 || space.UsedBytes > space.TotalBytes || space.AvailableBytes > space.TotalBytes-space.UsedBytes {
		t.Fatalf("invalid filesystem capacity: status=%d space=%+v", w.Code, space)
	}
	if strings.Contains(w.Body.String(), root) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("capacity must not disclose paths or be cached")
	}
	if !recordingRouteAllowed(httptest.NewRequest(http.MethodGet, "/api/v1/recordings/storage", nil)) {
		t.Fatal("recording users must be able to read capacity")
	}
}

func TestRecordingStorageUnavailableDoesNotFallBack(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(root, "missing"), file} {
		a := &app{recordingsRoot: path}
		w := httptest.NewRecorder()
		a.recordingStorage(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/storage", nil))
		var space recordingDiskSpace
		if err := json.Unmarshal(w.Body.Bytes(), &space); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK || space != (recordingDiskSpace{}) {
			t.Fatalf("missing storage must not fall back to another disk: %s", w.Body.String())
		}
	}
}

func TestRecordingStorageRejectsWritesAndUnauthenticatedReads(t *testing.T) {
	a := &app{recordingsRoot: t.TempDir()}
	w := httptest.NewRecorder()
	a.recordingStorage(w, httptest.NewRequest(http.MethodDelete, "/api/v1/recordings/storage", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodGet {
		t.Fatal("capacity endpoint must be read-only")
	}
	w = httptest.NewRecorder()
	a.auth(a.recordingStorage)(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/storage", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated capacity access returned %d", w.Code)
	}
}
