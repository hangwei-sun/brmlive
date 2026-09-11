package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	HTTPAddr         string  `json:"httpAddr"`
	MediaMTXRTMPBase string  `json:"mediaMTXRTMPBase"`
	RestartDelay     string  `json:"restartDelay"`
	Groups           []group `json:"groups"`
}

type group struct {
	Name         string    `json:"name"`
	Enabled      *bool     `json:"enabled"`
	Mode         string    `json:"mode"`
	InputURL     string    `json:"inputUrl"`
	InputFormat  string    `json:"inputFormat"`
	Address      string    `json:"address"`
	Port         int       `json:"port"`
	LocalAddress string    `json:"localAddress"`
	Programs     []program `json:"programs"`
}

type program struct {
	Name          string `json:"name"`
	ProgramNumber int    `json:"programNumber"`
	PublishPath   string `json:"publishPath"`
	Bitrate       string `json:"bitrate"`
	SampleRate    int    `json:"sampleRate"`
	Channels      int    `json:"channels"`
}

type groupStatus struct {
	Name         string `json:"name"`
	Mode         string `json:"mode"`
	InputFormat  string `json:"inputFormat"`
	Address      string `json:"address"`
	Port         int    `json:"port"`
	Enabled      bool   `json:"enabled"`
	Programs     int    `json:"programs"`
	Running      bool   `json:"running"`
	PID          int    `json:"pid,omitempty"`
	RestartCount int    `json:"restartCount"`
	StartedAt    string `json:"startedAt,omitempty"`
	LastExitAt   string `json:"lastExitAt,omitempty"`
	LastError    string `json:"lastError,omitempty"`
}

type runner struct {
	group group
	base  string
	delay time.Duration
	mu    sync.RWMutex
	stat  groupStatus
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	path := env("SPLITTER_CONFIG", "/etc/udp-splitter/config.json")
	cfg, err := loadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	delay := 5 * time.Second
	if cfg.RestartDelay != "" {
		if parsed, parseErr := time.ParseDuration(cfg.RestartDelay); parseErr == nil && parsed >= 0 {
			delay = parsed
		}
	}
	runners := make([]*runner, 0, len(cfg.Groups))
	for _, g := range cfg.Groups {
		if g.Enabled != nil && !*g.Enabled {
			continue
		}
		if len(g.Programs) == 0 {
			continue
		}
		if g.Name == "" {
			g.Name = fmt.Sprintf("%s:%d", g.Address, g.Port)
		}
		if g.Mode == "" {
			g.Mode = "mpts"
		}
		if g.InputFormat == "" {
			g.InputFormat = "mpegts"
		}
		if g.Mode != "mpts" && g.Mode != "single" {
			log.Printf("group %s has unsupported mode %q; using mpts", g.Name, g.Mode)
			g.Mode = "mpts"
		}
		runners = append(runners, &runner{group: g, base: strings.TrimRight(cfg.MediaMTXRTMPBase, "/"), delay: delay, stat: groupStatus{Name: g.Name, Mode: g.Mode, InputFormat: g.InputFormat, Address: g.Address, Port: g.Port, Enabled: true, Programs: len(g.Programs)}})
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for _, r := range runners {
		go r.loop(ctx)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]any{"ok": true}) })
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		out := make([]groupStatus, 0, len(runners))
		for _, r := range runners {
			out = append(out, r.snapshot())
		}
		writeJSON(w, 200, map[string]any{"groups": out})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, r := range runners {
			s := r.snapshot()
			fmt.Fprintf(w, "udp_splitter_group_running{name=%q} %d\n", s.Name, boolInt(s.Running))
			fmt.Fprintf(w, "udp_splitter_group_restarts{name=%q} %d\n", s.Name, s.RestartCount)
		}
	})
	addr := cfg.HTTPAddr
	if addr == "" {
		addr = env("SPLITTER_HTTP_ADDR", "127.0.0.1:9101")
	}
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("udp-splitter listening on %s, groups=%d", addr, len(runners))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func (r *runner) loop(ctx context.Context) {
	for {
		err := r.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		r.stat.Running = false
		r.stat.PID = 0
		r.stat.LastExitAt = time.Now().UTC().Format(time.RFC3339)
		if err != nil {
			r.stat.LastError = err.Error()
			log.Printf("group %s stopped: %v", r.group.Name, err)
		}
		r.stat.RestartCount++
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.delay):
		}
	}
}

func (r *runner) runOnce(ctx context.Context) error {
	args := ffmpegArgs(r.group, r.base)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var output lineBuffer
	cmd.Stdout = io.Discard
	cmd.Stderr = io.MultiWriter(os.Stderr, &output)
	if err := cmd.Start(); err != nil {
		return err
	}
	r.mu.Lock()
	r.stat.Running = true
	r.stat.PID = cmd.Process.Pid
	r.stat.StartedAt = time.Now().UTC().Format(time.RFC3339)
	r.stat.LastError = ""
	r.mu.Unlock()
	err := cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	if err == nil {
		return fmt.Errorf("ffmpeg exited normally")
	}
	if last := output.Last(); last != "" {
		return fmt.Errorf("%w: %s", err, last)
	}
	return err
}

func ffmpegArgs(g group, base string) []string {
	input := strings.TrimSpace(g.InputURL)
	if input == "" {
		input = fmt.Sprintf("udp://@%s:%d?fifo_size=5000000&overrun_nonfatal=1", g.Address, g.Port)
		if g.LocalAddress != "" {
			input += "&localaddr=" + g.LocalAddress
		}
	}
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "warning", "-fflags", "+genpts"}
	if format := ffmpegInputFormat(g.InputFormat); format != "" && format != "auto" {
		args = append(args, "-f", format)
	}
	args = append(args, "-i", input)
	for _, p := range g.Programs {
		bitrate := p.Bitrate
		if bitrate == "" {
			bitrate = "128k"
		}
		rate := p.SampleRate
		if rate == 0 {
			rate = 48000
		}
		channels := p.Channels
		if channels == 0 {
			channels = 2
		}
		path := strings.Trim(p.PublishPath, "/")
		mapArg := "0:a:0?"
		if g.Mode != "single" && p.ProgramNumber > 0 {
			mapArg = fmt.Sprintf("0:p:%d", p.ProgramNumber)
		}
		args = append(args, "-map", mapArg, "-vn", "-c:a", "aac", "-b:a", bitrate, "-ar", strconv.Itoa(rate), "-ac", strconv.Itoa(channels), "-f", "flv", base+"/"+path)
	}
	return args
}

func ffmpegInputFormat(raw string) string {
	switch format := strings.TrimSpace(strings.ToLower(raw)); format {
	case "adts":
		// FFmpeg names the AAC-ADTS demuxer "aac".
		return "aac"
	default:
		return format
	}
}

func loadConfig(path string) (config, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return config{}, err
	}
	var c config
	if err := json.Unmarshal(b, &c); err != nil {
		return config{}, err
	}
	if strings.TrimSpace(c.MediaMTXRTMPBase) == "" {
		c.MediaMTXRTMPBase = env("MEDIAMTX_RTMP_BASE", "rtmp://127.0.0.1:1935")
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = env("SPLITTER_HTTP_ADDR", "127.0.0.1:9101")
	}
	if c.MediaMTXRTMPBase == "" {
		return config{}, fmt.Errorf("mediaMTXRTMPBase is required")
	}
	return c, nil
}

type lineBuffer struct {
	mu   sync.Mutex
	line string
}

func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := strings.TrimSpace(string(p))
	if s != "" {
		lines := strings.Split(s, "\n")
		b.line = strings.TrimSpace(lines[len(lines)-1])
	}
	return len(p), nil
}
func (b *lineBuffer) Last() string      { b.mu.Lock(); defer b.mu.Unlock(); return b.line }
func (r *runner) snapshot() groupStatus { r.mu.RLock(); defer r.mu.RUnlock(); return r.stat }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
