package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGPUConfigFailClosed(t *testing.T) {
	t.Setenv("NEWS_ENCODER_URL", "")
	t.Setenv("NEWS_ENCODER_KEY", "")
	if b, _, err := gpuExportConfig(); err != nil || b != "" {
		t.Fatal("default should retain CPU")
	}
	t.Setenv("NEWS_ENCODER_URL", "http://user:private@encoder:8092")
	t.Setenv("NEWS_ENCODER_KEY", strings.Repeat("k", 24))
	if _, _, err := gpuExportConfig(); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("invalid configuration must fail without leaking credentials")
	}
}

func TestGPUTransferContract(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "original.mp4")
	if out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=red:s=160x90:d=1", "-c:v", "libx264", "-threads", "2", source).CombinedOutput(); err != nil {
		t.Fatalf("fixture %v %s", err, out)
	}
	original, _ := os.ReadFile(source)
	key := strings.Repeat("a", 32)
	token := strings.Repeat("k", 24)
	uploads, deleted := 0, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing authentication")
		}
		switch {
		case r.Method == "POST":
			var spec struct {
				Size  int64      `json:"size"`
				Parts []newsPart `json:"parts"`
			}
			json.NewDecoder(r.Body).Decode(&spec)
			if spec.Size != int64(len(original)) || len(spec.Parts) != 2 {
				t.Error("wrong upload manifest")
			}
			writeJSON(w, 200, map[string]string{"state": "waiting"})
		case r.Method == "PUT":
			data, _ := io.ReadAll(r.Body)
			if string(data) != string(original) || r.ContentLength != int64(len(original)) {
				t.Error("source not streamed intact")
			}
			uploads++
			writeJSON(w, 200, map[string]bool{"accepted": true})
		case r.Method == "DELETE":
			deleted = true
			writeJSON(w, 200, map[string]bool{"cancelled": true})
		case strings.Contains(r.URL.Path, "/files/"):
			w.Header().Set("Content-Type", "video/mp4")
			w.Write(original)
		default:
			writeJSON(w, 200, map[string]any{"state": "completed", "progress": 100, "stage": "Ready"})
		}
	}))
	defer server.Close()
	job := newsJob{Key: key, Parts: []newsPart{{Start: 0, End: 1}, {Start: 1, End: 2}}}
	stages := []string{}
	err := exportNewsGPU(context.Background(), server.URL, token, source, dir, &job, 2, func() { stages = append(stages, job.Stage) })
	if err != nil || uploads != 1 || !deleted || len(job.Files) != 2 || job.Progress != 90 || job.Encoder != "nvenc" {
		t.Fatalf("contract failed: %v %+v uploads=%v deleted=%v", err, job, uploads, deleted)
	}
	if len(stages) < 4 {
		t.Fatal("missing phase updates")
	}
	if after, _ := os.ReadFile(source); string(after) != string(original) {
		t.Fatal("original changed")
	}
}

func TestCPUProgressWithinClip(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	seen := false
	err := runNewsFFmpeg(context.Background(), []string{"-hide_banner", "-loglevel", "error", "-re", "-f", "lavfi", "-i", "color=red:s=160x90:r=25:d=2.5", "-f", "null", "-"}, 2.5, func(seconds float64) {
		if seconds > 0 && seconds < 2.5 {
			seen = true
		}
	})
	if err != nil || !seen {
		t.Fatalf("no within-clip progress: %v", err)
	}
}
