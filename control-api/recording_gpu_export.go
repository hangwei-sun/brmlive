package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The endpoint/key are operator settings, never browser input or job JSON.
func gpuExportConfig() (string, string, error) {
	base, key := strings.TrimRight(os.Getenv("NEWS_ENCODER_URL"), "/"), os.Getenv("NEWS_ENCODER_KEY")
	if base == "" && key == "" {
		return "", "", nil
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(key) < 24 || strings.ContainsAny(key, "\r\n") {
		return "", "", errors.New("GPU 导出服务配置不完整，请联系管理员")
	}
	return base, key, nil
}

func exportNewsGPU(ctx context.Context, base, token, source, dir string, job *newsJob, duration float64, save func()) error {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 15 * time.Second}).DialContext, ResponseHeaderTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Hour, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	endpoint := base + "/v1/exports/" + job.Key
	request := func(ctx context.Context, method, target string, body io.Reader, size int64) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return nil, errors.New("GPU 导出服务地址无效")
		}
		req.ContentLength = size
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := client.Do(req)
		if err != nil {
			return nil, errors.New("GPU 导出服务连接失败，请重试")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return nil, errors.New("GPU 导出服务未接受请求，请检查资源与配置")
		}
		return resp, nil
	}
	file, err := os.Open(source)
	if err != nil {
		return errors.New("录制文件不可读")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return errors.New("录制文件不可读")
	}
	spec := map[string]any{"parts": job.Parts, "duration": duration, "size": info.Size()}
	if os.Getenv("NEWS_SOURCE_CACHE") == "1" {
		digest := sha256.New()
		buffer := make([]byte, 1024*1024)
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			n, readErr := file.Read(buffer)
			if n > 0 {
				_, _ = digest.Write(buffer[:n])
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return errors.New("录制内容校验失败")
			}
		}
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		spec["sourceSha256"] = fmt.Sprintf("%x", digest.Sum(nil))
	}
	wire, _ := json.Marshal(spec)
	resp, err := request(ctx, http.MethodPost, endpoint, bytes.NewReader(wire), int64(len(wire)))
	if err != nil {
		return err
	}
	var created struct {
		SourceCached bool `json:"sourceCached"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&created); err != nil {
		resp.Body.Close()
		return errors.New("GPU 导出服务状态无效")
	}
	resp.Body.Close()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if r, e := request(cleanup, http.MethodDelete, endpoint, nil, 0); e == nil {
			r.Body.Close()
		}
	}()
	job.Encoder, job.Stage, job.Progress = "nvenc", "正在传输录制文件到 GPU 节点", 1
	save()
	// Stream the original once per task; never load a whole recording in RAM.
	if !created.SourceCached {
		resp, err = request(ctx, http.MethodPut, endpoint+"/source", file, info.Size())
		if err != nil {
			return err
		}
		resp.Body.Close()
	}
	current, err := os.Stat(source)
	if err != nil || current.Size() != info.Size() || !current.ModTime().Equal(info.ModTime()) {
		return errors.New("录制文件已变化，请重新提交")
	}
	job.Progress, job.Stage = 10, "GPU 排队中"
	save()
	var unavailableSince time.Time
	for {
		resp, err = request(ctx, http.MethodGet, endpoint, nil, 0)
		if err != nil {
			if unavailableSince.IsZero() {
				unavailableSince = time.Now()
			}
			if time.Since(unavailableSince) > 60*time.Second || ctx.Err() != nil {
				return err
			}
			job.Stage = "编码服务暂不可达，等待恢复（最多60秒）"
			save()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		unavailableSince = time.Time{}
		var status struct {
			State      string             `json:"state"`
			Progress   int                `json:"progress"`
			Stage      string             `json:"stage"`
			Timings    map[string]float64 `json:"timings"`
			Recoveries int                `json:"recoveries"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&status)
		resp.Body.Close()
		if err != nil || status.Progress < 0 || status.Progress > 100 {
			return errors.New("GPU 导出状态无效")
		}
		switch status.State {
		case "waiting", "uploading", "queued", "running", "completed", "failed", "cancelled":
		default:
			return errors.New("GPU 导出状态无效")
		}
		if status.State == "failed" || status.State == "cancelled" {
			return errors.New("GPU 硬件导出失败，请联系管理员检查编码服务")
		}
		if job.Timings == nil {
			job.Timings = map[string]float64{}
		}
		if status.Recoveries > 0 {
			job.Timings["gpu_recoveries"] = float64(status.Recoveries)
		}
		for _, key := range []string{"resource_queue_seconds", "encode_seconds"} {
			if value, ok := status.Timings[key]; ok && value >= 0 && value < 86400 {
				job.Timings["gpu_"+key] = value
			}
		}
		if status.State == "running" {
			job.Stage = "GPU 硬件编码 · " + status.Stage
		} else {
			job.Stage = "GPU 排队中"
		}
		job.Progress = max(job.Progress, 10+status.Progress*75/100)
		save()
		if status.State == "completed" {
			break
		}
		select {
		case <-ctx.Done():
			return errors.New("GPU 导出超时")
		case <-time.After(time.Second):
		}
	}
	for i := range job.Parts {
		name := "news-" + fmtNewsIndex(i+1) + ".mp4"
		job.Stage = "正在回传片段 " + strconv.Itoa(i+1) + "/" + strconv.Itoa(len(job.Parts))
		save()
		resp, err = request(ctx, http.MethodGet, endpoint+"/files/"+name, nil, 0)
		if err != nil {
			return err
		}
		tmp := filepath.Join(dir, "partial.mp4")
		output, e := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if e != nil {
			resp.Body.Close()
			return errors.New("片段保存失败")
		}
		n, e := io.Copy(output, io.LimitReader(resp.Body, 8*1024*1024*1024+1))
		resp.Body.Close()
		closeErr := output.Close()
		if e != nil || closeErr != nil || n < 1 || n > 8*1024*1024*1024 || (resp.ContentLength >= 0 && n != resp.ContentLength) {
			os.Remove(tmp)
			return errors.New("GPU 片段回传不完整")
		}
		actual, e := newsDuration(ctx, tmp)
		wanted := job.Parts[i].End - job.Parts[i].Start
		if e != nil || actual < wanted-0.25 || actual > wanted+0.25 {
			os.Remove(tmp)
			return errors.New("GPU 导出片段时长校验失败")
		}
		if e = os.Rename(tmp, filepath.Join(dir, name)); e != nil {
			return errors.New("片段保存失败")
		}
		job.Files = append(job.Files, name)
		job.Progress = 85 + (i+1)*5/len(job.Parts)
		save()
	}
	return nil
}

func fmtNewsIndex(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}

// CPU/audio compatibility also reports progress within the current clip.
func runNewsFFmpeg(ctx context.Context, args []string, duration float64, update func(float64)) error {
	args = append([]string{"-progress", "pipe:1", "-stats_period", "1"}, args...)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	last := time.Time{}
	for scanner.Scan() {
		if raw, ok := strings.CutPrefix(scanner.Text(), "out_time_us="); ok {
			us, e := strconv.ParseFloat(raw, 64)
			if e == nil && time.Since(last) >= time.Second {
				update(max(0, min(duration, us/1e6)))
				last = time.Now()
			}
		}
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}
	return scanner.Err()
}
