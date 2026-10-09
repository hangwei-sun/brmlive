package main

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type newsPart struct {
	Title string  `json:"title"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}
type newsJob struct {
	Key         string     `json:"key"`
	Owner       int64      `json:"ownerId"`
	RecordingID int64      `json:"recordingId"`
	Kind        string     `json:"kind"`
	State       string     `json:"state"`
	Progress    int        `json:"progress"`
	Message     string     `json:"message,omitempty"`
	Parts       []newsPart `json:"parts"`
	Files       []string   `json:"files"`
	CreatedAt   int64      `json:"createdAt"`
	ExpiresAt   int64      `json:"expiresAt"`
	RequestHash string     `json:"requestHash,omitempty"`
}
type newsJobInput struct {
	Key         string           `json:"key"`
	Owner       int64            `json:"ownerId"`
	RecordingID int64            `json:"recordingId"`
	Kind        string           `json:"kind"`
	Parts       []newsPart       `json:"parts"`
	Smart       *newsSmartConfig `json:"smart,omitempty"`
}

var newsKey = regexp.MustCompile(`^[a-f0-9]{32}$`)
var newsWork = struct {
	sync.Mutex
	active map[string]bool
}{active: map[string]bool{}}
var newsSlots = make(chan struct{}, 2)

func (a *app) newsRoot() string {
	if a.previewRoot != "" {
		return filepath.Join(filepath.Dir(a.previewRoot), "recording-jobs")
	}
	return filepath.Join(filepath.Dir(a.recordingsRoot), "recording-jobs")
}
func (a *app) newsDir(owner int64, key string) (string, error) {
	if owner < 1 || !newsKey.MatchString(key) {
		return "", errors.New("invalid job")
	}
	return filepath.Join(a.newsRoot(), strconv.FormatInt(owner, 10), key), nil
}
func writeNewsJob(dir string, job newsJob) error {
	wire, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "state.tmp"), wire, 0600); err != nil {
		return err
	}
	return os.Rename(filepath.Join(dir, "state.tmp"), filepath.Join(dir, "state.json"))
}
func readNewsJob(dir string) (newsJob, error) {
	var job newsJob
	wire, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return job, err
	}
	err = json.Unmarshal(wire, &job)
	return job, err
}
func (a *app) newsSource(id int64) (string, string, os.FileInfo, error) {
	var path, kind string
	err := a.db.QueryRow(`SELECT f.path,p.kind FROM recording_file f JOIN program p ON p.id=f.program_id WHERE f.id=?`, id).Scan(&path, &kind)
	if err != nil {
		return "", "", nil, err
	}
	path, err = a.recordingFilePath(path)
	if err != nil {
		return "", "", nil, err
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", nil, err
	}
	if _, err = a.recordingFilePath(real); err != nil {
		return "", "", nil, err
	}
	info, err := os.Stat(real)
	if err != nil || info == nil || !info.Mode().IsRegular() {
		return "", "", nil, errors.New("missing recording")
	}
	if time.Since(info.ModTime()) < 10*time.Second {
		return "", "", nil, errors.New("recording in progress")
	}
	return real, kind, info, nil
}
func newsDuration(ctx context.Context, path string) (float64, error) {
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path).Output()
	if err != nil {
		return 0, err
	}
	duration, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 {
		return 0, errors.New("invalid duration")
	}
	return duration, nil
}
func validateNewsParts(parts []newsPart, duration float64) error {
	if len(parts) < 1 || len(parts) > 30 {
		return errors.New("请选择 1 至 30 个片段")
	}
	var total float64
	for _, part := range parts {
		if math.IsNaN(part.Start) || math.IsNaN(part.End) || math.IsInf(part.Start, 0) || math.IsInf(part.End, 0) || part.Start < 0 || part.End <= part.Start || part.End > duration+0.1 || part.End-part.Start > 3600 {
			return errors.New("片段起止时间无效，单条最长 60 分钟")
		}
		if len([]rune(part.Title)) > 100 {
			return errors.New("片段标题最长 100 字")
		}
		total += part.End - part.Start
	}
	if total > 7200 {
		return errors.New("单次导出总时长不能超过 120 分钟")
	}
	return nil
}
func (a *app) recordingInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/v1/recordings/info/"), 10, 64)
	if err != nil || id < 1 {
		badRequest(w, "录制编号无效")
		return
	}
	source, kind, info, err := a.newsSource(id)
	if err != nil {
		recordingNotFound(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	duration, err := newsDuration(ctx, source)
	if err != nil {
		writeJSON(w, 503, map[string]string{"message": "无法读取录制时长，请检查媒体处理组件"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]any{"id": id, "kind": kind, "duration": duration, "size": info.Size()})
}
func (a *app) recordingJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.URL.Path == "/api/v1/recordings/jobs" {
		if r.Method == http.MethodGet {
			a.listNewsJobs(w, r)
			return
		}
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		a.createNewsJob(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/recordings/jobs/"), "/")
	owner, _ := strconv.ParseInt(r.URL.Query().Get("ownerId"), 10, 64)
	if len(parts) != 1 && len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	dir, err := a.newsDir(owner, parts[0])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	newsWork.Lock()
	job, err := readNewsJob(dir)
	if err == nil && job.Owner == owner && (job.State == "queued" || job.State == "running") && !newsWork.active[dir] {
		job.State, job.Message = "failed", "处理因服务重启而中断，请重新提交"
		_ = writeNewsJob(dir, job)
	}
	newsWork.Unlock()
	if err != nil || job.Owner != owner || job.Key != parts[0] || job.ExpiresAt <= time.Now().Unix() {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		writeJSON(w, 200, job)
		return
	}
	if parts[1] != "file" || job.State != "completed" {
		http.NotFound(w, r)
		return
	}
	name := parts[2]
	allowed := false
	for _, file := range job.Files {
		if file == name {
			allowed = true
		}
	}
	if !allowed || filepath.Base(name) != name {
		http.NotFound(w, r)
		return
	}
	file, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, name))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, name, info.ModTime(), file)
}
func (a *app) listNewsJobs(w http.ResponseWriter, r *http.Request) {
	owner, _ := strconv.ParseInt(r.URL.Query().Get("ownerId"), 10, 64)
	if owner < 1 {
		badRequest(w, "用户编号无效")
		return
	}
	dir := filepath.Join(a.newsRoot(), strconv.FormatInt(owner, 10))
	entries, _ := os.ReadDir(dir)
	jobs := []newsJob{}
	newsWork.Lock()
	defer newsWork.Unlock()
	for _, entry := range entries {
		if !entry.IsDir() || !newsKey.MatchString(entry.Name()) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		job, err := readNewsJob(path)
		if err != nil || job.Owner != owner || job.ExpiresAt <= time.Now().Unix() {
			continue
		}
		if (job.State == "queued" || job.State == "running") && !newsWork.active[path] {
			job.State, job.Message = "failed", "处理因服务重启而中断，请重新提交"
			_ = writeNewsJob(path, job)
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt > jobs[j].CreatedAt })
	if len(jobs) > 30 {
		jobs = jobs[:30]
	}
	writeJSON(w, 200, jobs)
}
func (a *app) createNewsJob(w http.ResponseWriter, r *http.Request) {
	var input newsJobInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	if err := decoder.Decode(&input); err != nil {
		badRequest(w, "拆条参数无效")
		return
	}
	dir, err := a.newsDir(input.Owner, input.Key)
	if err != nil || input.RecordingID < 1 || (input.Kind != "manual" && input.Kind != "smart") {
		badRequest(w, "拆条参数无效")
		return
	}
	newsWork.Lock()
	if old, err := readNewsJob(dir); err == nil {
		newsWork.Unlock()
		if old.ExpiresAt <= time.Now().Unix() {
			writeJSON(w, 410, map[string]string{"message": "任务已过期，请使用新的提交标识"})
			return
		}
		if old.RecordingID != input.RecordingID || old.Kind != input.Kind || old.RequestHash != newsRequestHash(input) {
			writeJSON(w, 409, map[string]string{"message": "提交标识已使用"})
			return
		}
		writeJSON(w, 200, old)
		return
	}
	if len(newsWork.active) >= 8 {
		newsWork.Unlock()
		writeJSON(w, 429, map[string]string{"message": "拆条队列繁忙，请稍后重试"})
		return
	}
	ownerDir := filepath.Dir(dir)
	for active := range newsWork.active {
		if filepath.Dir(active) == ownerDir {
			newsWork.Unlock()
			writeJSON(w, 429, map[string]string{"message": "已有拆条任务正在处理，请等待完成"})
			return
		}
	}
	newsWork.active[dir] = true
	newsWork.Unlock()
	accepted := false
	defer func() {
		if !accepted {
			newsWork.Lock()
			delete(newsWork.active, dir)
			newsWork.Unlock()
		}
	}()
	// Existing generated exports expire after 24h. Only owned, valid job directories
	// are eligible for cleanup; original recordings are never touched.
	entries, _ := os.ReadDir(ownerDir)
	daily := 0
	for _, entry := range entries {
		if !entry.IsDir() || !newsKey.MatchString(entry.Name()) {
			continue
		}
		other := filepath.Join(ownerDir, entry.Name())
		old, err := readNewsJob(other)
		if err != nil || old.Owner != input.Owner {
			continue
		}
		if old.CreatedAt > time.Now().Add(-24*time.Hour).Unix() {
			daily++
		}
		if old.ExpiresAt < time.Now().Unix() {
			newsWork.Lock()
			active := newsWork.active[other]
			newsWork.Unlock()
			if !active {
				_ = os.RemoveAll(other)
			}
		}
	}
	if daily >= 30 {
		writeJSON(w, 429, map[string]string{"message": "24 小时内最多提交 30 个拆条任务"})
		return
	}
	source, kind, info, err := a.newsSource(input.RecordingID)
	if err != nil {
		recordingNotFound(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	duration, err := newsDuration(ctx, source)
	if err != nil {
		writeJSON(w, 503, map[string]string{"message": "无法读取录制时长"})
		return
	}
	if input.Kind == "smart" {
		if duration > 10800 {
			badRequest(w, "智能拆条支持 3 小时以内的整期新闻节目")
			return
		}
		if input.Smart == nil || input.Smart.validate() != nil {
			writeJSON(w, 503, map[string]string{"message": "智能拆条服务尚未配置，请联系管理员"})
			return
		}
	} else if err := validateNewsParts(input.Parts, duration); err != nil {
		badRequest(w, err.Error())
		return
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		writeJSON(w, 503, map[string]string{"message": "拆条缓存不可写"})
		return
	}
	_, _, free, err := recordingFilesystemSpace(dir)
	reserve := uint64(512 * 1024 * 1024)
	if input.Kind != "smart" {
		for _, part := range input.Parts {
			reserve += uint64(float64(info.Size()) * (part.End - part.Start) / duration * 3)
		}
	}
	if err != nil || free < reserve {
		_ = os.Remove(dir)
		writeJSON(w, 507, map[string]string{"message": "拆条缓存可用空间不足"})
		return
	}
	job := newsJob{Key: input.Key, Owner: input.Owner, RecordingID: input.RecordingID, Kind: input.Kind, State: "queued", Parts: input.Parts, Files: []string{}, CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(24 * time.Hour).Unix()}
	job.RequestHash = newsRequestHash(input)
	newsWork.Lock()
	err = writeNewsJob(dir, job)
	newsWork.Unlock()
	if err != nil {
		writeJSON(w, 503, map[string]string{"message": "无法保存拆条任务"})
		return
	}
	accepted = true
	go a.runNewsJob(dir, job, input.Smart, source, kind, info, duration)
	writeJSON(w, http.StatusAccepted, job)
}

// Fingerprint only user intent, never service credentials or mutable result parts.
func newsRequestHash(input newsJobInput) string {
	wire, _ := json.Marshal(struct {
		ID    int64
		Kind  string
		Parts []newsPart
	}{input.RecordingID, input.Kind, input.Parts})
	return fmt.Sprintf("%x", sha256.Sum256(wire))
}
func (a *app) runNewsJob(dir string, job newsJob, smart *newsSmartConfig, source, kind string, fingerprint os.FileInfo, duration float64) {
	defer func() { newsWork.Lock(); delete(newsWork.active, dir); newsWork.Unlock() }()
	newsSlots <- struct{}{}
	defer func() { <-newsSlots }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	save := func() { newsWork.Lock(); _ = writeNewsJob(dir, job); newsWork.Unlock() }
	fail := func(message string) { job.State, job.Message = "failed", message; save() }
	job.State = "running"
	save()
	info, err := os.Stat(source)
	if err != nil || info.Size() != fingerprint.Size() || !info.ModTime().Equal(fingerprint.ModTime()) {
		fail("录制文件已变化，请重新打开文件")
		return
	}
	if job.Kind == "smart" {
		parts, err := analyseNews(ctx, source, dir, duration, *smart, func(progress int) { job.Progress = progress; save() })
		if err != nil {
			fail(err.Error())
			return
		}
		job.Parts, job.State, job.Progress = parts, "completed", 100
		save()
		return
	}
	for i, part := range job.Parts {
		ext := ".mp4"
		if kind == "audio" {
			ext = ".m4a"
		}
		name := fmt.Sprintf("news-%02d%s", i+1, ext)
		tmp := filepath.Join(dir, "partial"+ext)
		args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-ss", fmt.Sprintf("%.3f", part.Start), "-i", source, "-t", fmt.Sprintf("%.3f", part.End-part.Start), "-map", "0:a:0?"}
		if kind != "audio" {
			args = append(args, "-map", "0:v:0?", "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-threads", "2")
		}
		args = append(args, "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", tmp)
		if err = exec.CommandContext(ctx, "ffmpeg", args...).Run(); err != nil {
			_ = os.Remove(tmp)
			fail("片段导出失败，请联系管理员检查媒体处理服务")
			return
		}
		if err = os.Rename(tmp, filepath.Join(dir, name)); err != nil {
			fail("片段保存失败")
			return
		}
		job.Files = append(job.Files, name)
		job.Progress = (i + 1) * 90 / len(job.Parts)
		save()
	}
	if job.Kind != "preview" {
		if err := zipNews(dir, job.Files, job.Parts); err != nil {
			fail("打包失败，请重新提交")
			return
		}
		job.Files = append(job.Files, "news-clips.zip")
	}
	job.State, job.Progress = "completed", 100
	save()
}
func zipNews(dir string, files []string, parts []newsPart) error {
	file, err := os.Create(filepath.Join(dir, "news-clips.zip"))
	if err != nil {
		return err
	}
	defer file.Close()
	archive := zip.NewWriter(file)
	for _, name := range files {
		header := &zip.FileHeader{Name: name, Method: zip.Store}
		writer, err := archive.CreateHeader(header)
		if err != nil {
			archive.Close()
			return err
		}
		source, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			archive.Close()
			return err
		}
		_, err = io.Copy(writer, source)
		source.Close()
		if err != nil {
			archive.Close()
			return err
		}
	}
	writer, err := archive.Create("片段清单.json")
	if err != nil {
		archive.Close()
		return err
	}
	if err = json.NewEncoder(writer).Encode(parts); err != nil {
		archive.Close()
		return err
	}
	return archive.Close()
}
