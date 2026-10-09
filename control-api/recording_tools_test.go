package main

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewsPartsRejectInvalidRanges(t *testing.T) {
	for _, parts := range [][]newsPart{
		{}, {{Start: -1, End: 10}}, {{Start: 10, End: 10}}, {{Start: 0, End: 3601}},
		{{Start: 0, End: 21}}, {{Start: math.NaN(), End: 10}}, {{Start: 0, End: math.Inf(1)}},
	} {
		if validateNewsParts(parts, 20) == nil {
			t.Fatalf("accepted invalid ranges: %+v", parts)
		}
	}
	if err := validateNewsParts([]newsPart{{Title: "新闻", Start: 0.25, End: 10.8}}, 20); err != nil {
		t.Fatal(err)
	}
}

func TestSemanticGroupsUseOnlyExistingTimestamps(t *testing.T) {
	sentences := []newsSentence{{Start: 0, End: 2, Text: "片头"}, {Start: 2, End: 4, Text: "第一条导语"}, {Start: 4, End: 8, Text: "第一条报道"}, {Start: 8, End: 10, Text: "下一条"}}
	parts, err := validateNewsGroups([]newsGroup{{Title: "第一条", First: 1, Last: 2}, {Title: "第二条", First: 3, Last: 3}}, sentences)
	if err != nil || parts[0].Start != 2 || parts[0].End != 8 {
		t.Fatalf("wrong timestamp mapping: %+v %v", parts, err)
	}
	for _, groups := range [][]newsGroup{
		{{Title: "bad", First: -1, Last: 1}}, {{Title: "bad", First: 0, Last: 8}},
		{{Title: "one", First: 0, Last: 2}, {Title: "overlap", First: 2, Last: 3}}, {{First: 0, Last: 1}},
	} {
		if _, err := validateNewsGroups(groups, sentences); err == nil {
			t.Fatal("accepted invalid AI output")
		}
	}
}

func TestASRAndChatContractsAndCredentialRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing private credential")
		}
		if r.URL.Path == "/asr" {
			if err := r.ParseMultipartForm(30 * 1024 * 1024); err != nil {
				t.Error(err)
			}
			defer r.MultipartForm.RemoveAll()
			if r.FormValue("response_format") != "verbose_json" {
				t.Error("timestamp response format missing")
			}
			writeJSON(w, 200, map[string]any{"segments": []newsSentence{{Start: 1, End: 3, Text: "新闻"}}})
			return
		}
		var input map[string]any
		_ = json.NewDecoder(r.Body).Decode(&input)
		if input["model"] != "test-model" {
			t.Error("wrong semantic model")
		}
		writeJSON(w, 200, map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": `{"stories":[{"title":"新闻一","first":0,"last":0}]}`}}}})
	}))
	defer server.Close()
	config := newsSmartConfig{ASRURL: server.URL + "/asr", ASRKey: "test-key", ASRModel: "test-model", ChatURL: server.URL + "/chat", ChatKey: "test-key", ChatModel: "test-model"}
	path := filepath.Join(t.TempDir(), "audio.wav")
	_ = os.WriteFile(path, []byte("test audio"), 0600)
	sentences, err := transcribeNews(context.Background(), server.Client(), config, path, 600, 10)
	if err != nil || sentences[0].Start != 601 || sentences[0].End != 603 {
		t.Fatalf("chunk offset wrong: %+v %v", sentences, err)
	}
	parts, err := groupNews(context.Background(), server.Client(), config, sentences)
	if err != nil || parts[0].Start != 601 {
		t.Fatal(err)
	}
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "test-key private internal path", 500) }))
	defer failed.Close()
	config.ASRURL = failed.URL
	_, err = transcribeNews(context.Background(), failed.Client(), config, path, 0, 10)
	if err == nil || strings.Contains(err.Error(), "test-key") || strings.Contains(err.Error(), "internal") {
		t.Fatal("upstream error was not sanitized")
	}
}

func TestRecordingJobOwnershipExpiryAndRestart(t *testing.T) {
	a := &app{previewRoot: filepath.Join(t.TempDir(), "previews")}
	key := strings.Repeat("a", 32)
	dir, _ := a.newsDir(1, key)
	_ = os.MkdirAll(dir, 0700)
	job := newsJob{Key: key, Owner: 1, State: "running", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	if err := writeNewsJob(dir, job); err != nil {
		t.Fatal(err)
	}
	request := func(owner string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		a.recordingJobs(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/jobs/"+key+"?ownerId="+owner, nil))
		return w
	}
	if w := request("2"); w.Code != 404 {
		t.Fatal("another owner read a job")
	}
	w := request("1")
	var got newsJob
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != 200 || got.State != "failed" {
		t.Fatal("interrupted job was not reported")
	}
	job.ExpiresAt = time.Now().Add(-time.Hour).Unix()
	_ = writeNewsJob(dir, job)
	if request("1").Code != 404 {
		t.Fatal("expired job is accessible")
	}
	if _, err := a.newsDir(1, "../../file"); err == nil {
		t.Fatal("job path traversal allowed")
	}
}

func TestNewsExportRealFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("FFmpeg unavailable")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe unavailable")
	}
	root := t.TempDir()
	source := filepath.Join(root, "full.mp4")
	cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=blue:s=160x90:r=25", "-f", "lavfi", "-i", "sine=frequency=500:sample_rate=16000", "-t", "3", "-c:v", "libx264", "-c:a", "aac", "-y", source)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %s %v", output, err)
	}
	old := time.Now().Add(-time.Minute)
	_ = os.Chtimes(source, old, old)
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO program(id,name,slug,source_url,kind,created_at,updated_at) VALUES(1,'News','news','http://example.test/live','video','now','now')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO recording_file(id,program_id,path,size_bytes,created_at) VALUES(1,1,?,100,'now')`, source)
	if err != nil {
		t.Fatal(err)
	}
	a := &app{db: db, recordingsRoot: root, previewRoot: filepath.Join(root, "cache", "previews")}
	a.previewJobs = map[int64]*previewJob{}
	a.previewSem = make(chan struct{}, 1)
	inputInfo, _ := os.Stat(source)
	previewFile, ready, err := a.previewPath(1, source, inputInfo.ModTime())
	if err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(10 * time.Second); !ready && time.Now().Before(until); {
		time.Sleep(50 * time.Millisecond)
		previewFile, ready, err = a.previewPath(1, source, inputInfo.ModTime())
		if err != nil {
			t.Fatal(err)
		}
	}
	previewDuration, err := newsDuration(context.Background(), previewFile)
	if !ready || err != nil || math.Abs(previewDuration-3) > 0.1 {
		t.Fatal("whole-program preview is incomplete")
	}
	wPreview := httptest.NewRecorder()
	rPreview := httptest.NewRequest(http.MethodGet, "/api/v1/recordings/file/1?preview=1", nil)
	rPreview.Header.Set("Range", "bytes=0-99")
	a.recordingFileContent(wPreview, rPreview)
	if wPreview.Code != 206 || wPreview.Body.Len() != 100 {
		t.Fatal("whole preview Range failed")
	}
	key := strings.Repeat("b", 32)
	wire := fmtNewsInput(key, 1, "manual", []newsPart{{Title: "新闻条目", Start: 0.35, End: 1.75}})
	w := httptest.NewRecorder()
	a.createNewsJob(w, httptest.NewRequest(http.MethodPost, "/api/v1/recordings/jobs", strings.NewReader(wire)))
	if w.Code != 202 {
		t.Fatalf("submit: %s", w.Body.String())
	}
	dir, _ := a.newsDir(1, key)
	var job newsJob
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		newsWork.Lock()
		job, _ = readNewsJob(dir)
		newsWork.Unlock()
		if job.State == "completed" || job.State == "failed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if job.State != "completed" {
		t.Fatalf("export did not complete: %+v", job)
	}
	w = httptest.NewRecorder()
	a.createNewsJob(w, httptest.NewRequest(http.MethodPost, "/api/v1/recordings/jobs", strings.NewReader(wire)))
	if w.Code != 200 {
		t.Fatal("same request key should recover the original task")
	}
	w = httptest.NewRecorder()
	changed := fmtNewsInput(key, 1, "manual", []newsPart{{Title: "changed", Start: 0, End: 2}})
	a.createNewsJob(w, httptest.NewRequest(http.MethodPost, "/api/v1/recordings/jobs", strings.NewReader(changed)))
	if w.Code != 409 {
		t.Fatal("same key accepted different clip ranges")
	}
	duration, err := newsDuration(context.Background(), filepath.Join(dir, "news-01.mp4"))
	if err != nil || math.Abs(duration-1.4) > 0.15 {
		t.Fatalf("inaccurate cut duration=%f err=%v", duration, err)
	}
	archive, err := zip.OpenReader(filepath.Join(dir, "news-clips.zip"))
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if len(archive.File) != 2 {
		t.Fatal("ZIP missing clip or manifest")
	}
	w = httptest.NewRecorder()
	a.recordingJobs(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/jobs/"+key+"/file/news-01.mp4?ownerId=1", nil))
	if w.Code != 200 || w.Body.Len() == 0 {
		t.Fatal("download failed")
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/recordings/jobs/"+key+"/file/news-01.mp4?ownerId=1", nil)
	r.Header.Set("Range", "bytes=0-99")
	w = httptest.NewRecorder()
	a.recordingJobs(w, r)
	if w.Code != 206 || w.Body.Len() != 100 {
		t.Fatal("range download failed")
	}
	w = httptest.NewRecorder()
	a.recordingJobs(w, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/jobs/"+key+"/file/../state.json?ownerId=1", nil))
	if w.Code != 404 {
		t.Fatal("private state exposed")
	}
	f, err := os.Open(source)
	if err != nil {
		t.Fatal("original file modified")
	}
	defer f.Close()
	_, _ = io.Copy(io.Discard, f)
}
func fmtNewsInput(key string, id int64, kind string, parts []newsPart) string {
	wire, _ := json.Marshal(newsJobInput{Key: key, Owner: 1, RecordingID: id, Kind: kind, Parts: parts})
	return string(wire)
}
