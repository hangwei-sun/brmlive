package main

import (
	"net/http"
	"os"
)

type recordingDiskSpace struct {
	Available      bool   `json:"available"`
	TotalBytes     uint64 `json:"totalBytes"`
	UsedBytes      uint64 `json:"usedBytes"`
	AvailableBytes uint64 `json:"availableBytes"`
}

func (a *app) recordingStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "Method not allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	root := a.recordingsRoot
	if root == "" {
		root = "/recordings"
	}
	// Query the configured mount only. Never fall back to the service's root disk
	// or scan recording files; missing directories must not report unrelated capacity.
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		writeJSON(w, http.StatusOK, recordingDiskSpace{})
		return
	}
	total, free, available, err := recordingFilesystemSpace(root)
	if err != nil || total == 0 || free > total || available > free {
		writeJSON(w, http.StatusOK, recordingDiskSpace{})
		return
	}
	writeJSON(w, http.StatusOK, recordingDiskSpace{
		Available: true, TotalBytes: total, UsedBytes: total - free, AvailableBytes: available,
	})
}
