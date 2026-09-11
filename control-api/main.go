package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type app struct {
	db                   *sql.DB
	mtx                  *mediaMTX
	crypt                *secretBox
	location             *time.Location
	recordingsRoot       string
	previewRoot          string
	previewMu            sync.Mutex
	previewJobs          map[int64]*previewJob
	previewSem           chan struct{}
	mediaSourceProxyBase string
	mediaHLSBase         string
	splitterStatusURL    string
	sessions             map[string]time.Time
	sessionUsers         map[string]string
	sessionRoles         map[string]string
	sessMu               sync.RWMutex
	stateMu              sync.Mutex
	active               map[int64]bool
	activeKnown          map[int64]bool
	signalMu             sync.RWMutex
	signalCache          map[int64]signalProbe
	brandMu              sync.RWMutex
	brandName            string
	brandLogoText        string
	brandLogoURL         string
	mediaHealthMu        sync.Mutex
	mediaHealth          map[string]mediaPathHealth
	relayMu              sync.Mutex
	relays               map[relayKey]*relayRuntime
}

type mediaPathHealth struct {
	staleSince  time.Time
	lastRestart time.Time
}

const (
	mediaPathStaleAfter = 90 * time.Second
	mediaPathRetryAfter = 2 * time.Minute
)

type signalProbe struct {
	Status  string
	Reason  string
	Checked time.Time
}

type previewJob struct {
	status    string
	errText   string
	sourceMod time.Time
}

type relayKey struct {
	programID     int64
	destinationID int64
}

type relayRuntime struct {
	cancel    context.CancelFunc
	dest      string
	state     string
	lastError string
	startedAt time.Time
}

type secretBox struct{ key []byte }

func newSecretBox(raw string) *secretBox {
	if raw == "" {
		raw = "brmlive-local-development-key-change-me"
	}
	h := sha256.Sum256([]byte(raw))
	return &secretBox{key: h[:]}
}

func (s *secretBox) seal(value string) (string, error) {
	b, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	out := g.Seal(nonce, nonce, []byte(value), nil)
	return base64.RawStdEncoding.EncodeToString(out), nil
}

func (s *secretBox) open(value string) (string, error) {
	b, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	raw, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil || len(raw) < g.NonceSize() {
		return "", errors.New("invalid encrypted value")
	}
	plain, err := g.Open(nil, raw[:g.NonceSize()], raw[g.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

type program struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	Slug          string `json:"slug"`
	Kind          string `json:"kind"`
	SourceURL     string `json:"sourceUrl"`
	Enabled       bool   `json:"enabled"`
	RecordEnabled bool   `json:"recordEnabled"`
	CreatedAt     string `json:"createdAt"`
	UpdatedAt     string `json:"updatedAt"`
}

type destination struct {
	ID                 int64  `json:"id"`
	Name               string `json:"name"`
	URL                string `json:"url"`
	Protocol           string `json:"protocol"`
	KeyMasked          string `json:"streamKeyMasked"`
	Enabled            bool   `json:"enabled"`
	StreamStatus       string `json:"streamStatus,omitempty"`
	StreamStatusReason string `json:"streamStatusReason,omitempty"`
}

type schedule struct {
	ID        int64  `json:"id"`
	ProgramID int64  `json:"programId"`
	Weekday   int    `json:"weekday"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Enabled   bool   `json:"enabled"`
}

type programInput struct {
	Name          string `json:"name"`
	Slug          string `json:"slug"`
	Kind          string `json:"kind"`
	SourceURL     string `json:"sourceUrl"`
	Enabled       *bool  `json:"enabled"`
	RecordEnabled *bool  `json:"recordEnabled"`
}

type destinationInput struct {
	Name           string `json:"name"`
	URL            string `json:"url"`
	Protocol       string `json:"protocol"`
	StreamKey      string `json:"streamKey"`
	ClearStreamKey bool   `json:"clearStreamKey"`
	Enabled        *bool  `json:"enabled"`
}

type scheduleInput struct {
	Weekday   int    `json:"weekday"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Enabled   *bool  `json:"enabled"`
}

type streamAccess struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	Slug           string `json:"slug"`
	Enabled        bool   `json:"enabled"`
	ExpiresAt      string `json:"expiresAt,omitempty"`
	PublishURL     string `json:"publishUrl,omitempty"`
	PullURL        string `json:"pullUrl,omitempty"`
	HLSURL         string `json:"hlsUrl,omitempty"`
	PublishURLHint string `json:"publishUrlHint"`
	PullURLHint    string `json:"pullUrlHint"`
	HLSURLHint     string `json:"hlsUrlHint"`
	Status         string `json:"status"`
	Readers        int    `json:"readers"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
}

type streamAccessInput struct {
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	Enabled   *bool  `json:"enabled"`
	ExpiresAt string `json:"expiresAt"`
}

type streamAccessDB struct {
	ID            int64
	Name          string
	Slug          string
	PublishUser   string
	PublishCipher string
	ReadUser      string
	ReadCipher    string
	Enabled       bool
	ExpiresAt     sql.NullString
	CreatedAt     string
	UpdatedAt     string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	port := env("CONTROL_API_PORT", "8080")
	dbPath := env("CONTROL_DB_PATH", "/data/control.db")
	mtxURL := strings.TrimRight(env("MEDIAMTX_API_URL", "http://mediamtx:9997"), "/")
	loc, err := time.LoadLocation(env("TZ", "Asia/Shanghai"))
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	// The local deployment uses a Windows bind mount. A single SQLite
	// connection avoids intermittent "unable to open database file" errors
	// when status polling, scheduling and indexing overlap.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	if err := migrate(db); err != nil {
		log.Fatal(err)
	}

	a := &app{db: db, mtx: &mediaMTX{baseURL: mtxURL, client: &http.Client{Timeout: 15 * time.Second}}, crypt: newSecretBox(os.Getenv("STREAM_KEY_ENCRYPTION_KEY")), location: loc, recordingsRoot: env("RECORDINGS_ROOT", "/recordings"), previewRoot: env("PREVIEW_ROOT", "/data/previews"), mediaSourceProxyBase: strings.TrimRight(env("MEDIAMTX_HLS_PROXY_BASE_URL", ""), "/"), mediaHLSBase: strings.TrimRight(env("MEDIAMTX_HLS_INTERNAL_URL", "http://mediamtx:8888"), "/"), splitterStatusURL: strings.TrimRight(env("SPLITTER_STATUS_URL", ""), "/"), sessions: map[string]time.Time{}, sessionUsers: map[string]string{}, sessionRoles: map[string]string{}, active: map[int64]bool{}, activeKnown: map[int64]bool{}, signalCache: map[int64]signalProbe{}, previewJobs: map[int64]*previewJob{}, previewSem: make(chan struct{}, 1), mediaHealth: map[string]mediaPathHealth{}, relays: map[relayKey]*relayRuntime{}}
	a.restoreSessions()
	a.loadBrandCache()
	// Reconcile all paths after MediaMTX becomes available so persisted global
	// recording settings are applied after a control service restart.
	go func() {
		for i := 0; i < 12; i++ {
			if err := a.syncAllPrograms(context.Background()); err == nil {
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	go a.scheduler(context.Background())
	go a.indexRecordings(context.Background())
	go a.signalMonitor(context.Background())
	go a.streamAccessMonitor(context.Background())
	go a.mediaMTXMonitor(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.health)
	mux.HandleFunc("/api/v1/media-auth", a.mediaAuth)
	mux.HandleFunc("/api/v1/branding", a.branding)
	mux.HandleFunc("/hls/", a.hlsProxy)
	mux.HandleFunc("/api/v1/auth/login", a.login)
	mux.HandleFunc("/api/v1/auth/logout", a.auth(a.logout))
	mux.HandleFunc("/api/v1/auth/me", a.auth(a.me))
	mux.HandleFunc("/api/v1/auth/password", a.auth(a.changePassword))
	mux.HandleFunc("/api/v1/auth/users", a.auth(a.users))
	mux.HandleFunc("/api/v1/settings", a.auth(a.settings))
	mux.HandleFunc("/api/v1/programs", a.auth(a.programs))
	mux.HandleFunc("/api/v1/programs/", a.auth(a.programSubroutes))
	mux.HandleFunc("/api/v1/destinations", a.auth(a.destinations))
	mux.HandleFunc("/api/v1/destinations/", a.auth(a.destinationSubroutes))
	mux.HandleFunc("/api/v1/stream-access", a.auth(a.streamAccesses))
	mux.HandleFunc("/api/v1/stream-access/", a.auth(a.streamAccessSubroutes))
	mux.HandleFunc("/api/v1/schedules/", a.auth(a.scheduleSubroutes))
	mux.HandleFunc("/api/v1/status", a.auth(a.status))
	mux.HandleFunc("/api/v1/splitter/status", a.auth(a.splitterStatus))
	mux.HandleFunc("/api/v1/recordings", a.auth(a.recordings))
	mux.HandleFunc("/api/v1/recordings/preview/", a.auth(a.recordingPreviewStatus))
	mux.HandleFunc("/api/v1/recordings/", a.auth(a.recordingFileContent))
	mux.HandleFunc("/api/v1/executions", a.auth(a.executions))
	mux.HandleFunc("/api/v1/events", a.auth(a.events))
	// The console is deployed as a single bundled HTML file. Avoid stale browser
	// copies after a control-api image upgrade so new dashboard cards appear.
	mux.Handle("/", noCache(http.FileServer(http.Dir(env("WEB_ROOT", "/app/web")))))

	server := &http.Server{Addr: ":" + port, Handler: cors(logging(mux))}
	log.Printf("control-api listening on :%s, MediaMTX=%s, TZ=%s", port, mtxURL, loc)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		next.ServeHTTP(w, r)
	})
}

func (a *app) mediaAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var in struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Action   string `json:"action"`
		Path     string `json:"path"`
	}
	if !decode(r, &in) {
		unauthorized(w)
		return
	}
	// Generated stream-access paths require their own credentials.
	var d streamAccessDB
	var enabled int
	if err := a.db.QueryRow(`SELECT id,name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,expires_at,created_at,updated_at FROM stream_access WHERE slug=?`, in.Path).Scan(&d.ID, &d.Name, &d.Slug, &d.PublishUser, &d.PublishCipher, &d.ReadUser, &d.ReadCipher, &enabled, &d.ExpiresAt, &d.CreatedAt, &d.UpdatedAt); err == nil {
		if enabled == 0 || hasExpired(d.ExpiresAt) {
			unauthorized(w)
			return
		}
		cipherText := d.ReadCipher
		expectedUser := d.ReadUser
		if in.Action == "publish" {
			cipherText, expectedUser = d.PublishCipher, d.PublishUser
		}
		pass, openErr := a.crypt.open(cipherText)
		if openErr == nil && in.User == expectedUser && in.Password == pass {
			writeJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		unauthorized(w)
		return
	}
	// Existing fixed HLS programs remain readable without credentials. The
	// splitter's publisher-fed paths are allowed to publish from the LAN relay.
	if (in.Action == "read" || in.Action == "playback") && in.User == "" && in.Password == "" {
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	if in.Action == "publish" && strings.HasPrefix(in.Path, "audio_encoder_") {
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	unauthorized(w)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS program (
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, slug TEXT NOT NULL UNIQUE,
 kind TEXT NOT NULL DEFAULT 'video', source_url TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
 record_enabled INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS destination (
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, url TEXT NOT NULL,
 protocol TEXT NOT NULL DEFAULT 'rtmp', stream_key_cipher TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS program_destination (
 program_id INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
 destination_id INTEGER NOT NULL REFERENCES destination(id) ON DELETE CASCADE,
 enabled INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(program_id, destination_id)
);
CREATE TABLE IF NOT EXISTS recording_schedule (
 id INTEGER PRIMARY KEY AUTOINCREMENT, program_id INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
 weekday INTEGER NOT NULL CHECK(weekday BETWEEN 0 AND 6), start_time TEXT NOT NULL, end_time TEXT NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS recording_execution (
 id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id INTEGER NOT NULL REFERENCES recording_schedule(id) ON DELETE CASCADE,
 program_id INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE, started_at TEXT NOT NULL,
 ended_at TEXT, status TEXT NOT NULL, error TEXT
);
CREATE TABLE IF NOT EXISTS recording_file (
 id INTEGER PRIMARY KEY AUTOINCREMENT, program_id INTEGER NOT NULL REFERENCES program(id) ON DELETE CASCADE,
 path TEXT NOT NULL UNIQUE, size_bytes INTEGER NOT NULL DEFAULT 0, duration_seconds REAL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS system_event (
 id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT NOT NULL, kind TEXT NOT NULL, message TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_user (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'admin');
CREATE TABLE IF NOT EXISTS auth_session (
 token TEXT PRIMARY KEY, username TEXT NOT NULL REFERENCES admin_user(username) ON DELETE CASCADE,
 role TEXT NOT NULL, expires_at TEXT
);
CREATE TABLE IF NOT EXISTS global_setting (
 id INTEGER PRIMARY KEY CHECK(id=1), brand_name TEXT NOT NULL DEFAULT 'brmlive 运维控制台', logo_text TEXT NOT NULL DEFAULT 'B', logo_url TEXT NOT NULL DEFAULT '',
 session_ttl_minutes INTEGER NOT NULL DEFAULT 1440,
 record_format TEXT NOT NULL DEFAULT 'fmp4', record_bitrate_kbps INTEGER NOT NULL DEFAULT 0,
 record_part_duration TEXT NOT NULL DEFAULT '1s', record_segment_duration TEXT NOT NULL DEFAULT '30s',
 record_delete_after TEXT NOT NULL DEFAULT '0s', record_max_part_size TEXT NOT NULL DEFAULT '50M',
 updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_access (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 name TEXT NOT NULL,
 slug TEXT NOT NULL UNIQUE,
 publish_user TEXT NOT NULL,
 publish_pass_cipher TEXT NOT NULL,
 read_user TEXT NOT NULL,
 read_pass_cipher TEXT NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1,
 expires_at TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
`)
	if err != nil {
		return err
	}
	user, pass := env("ADMIN_USER", "admin"), env("ADMIN_PASSWORD", "change-me")
	h := sha256.Sum256([]byte(pass))
	_, err = db.Exec(`INSERT INTO admin_user(username,password_hash) VALUES(?,?) ON CONFLICT(username) DO NOTHING`, user, fmt.Sprintf("%x", h[:]))
	if err != nil {
		return err
	}
	for _, column := range []struct{ name, definition string }{
		{"role", "TEXT NOT NULL DEFAULT 'admin'"},
		{"brand_name", "TEXT NOT NULL DEFAULT 'brmlive 运维控制台'"},
		{"session_ttl_minutes", "INTEGER NOT NULL DEFAULT 1440"},
		{"record_format", "TEXT NOT NULL DEFAULT 'fmp4'"},
		{"record_bitrate_kbps", "INTEGER NOT NULL DEFAULT 0"},
		{"record_duration", "TEXT NOT NULL DEFAULT '30m'"},
		{"record_part_duration", "TEXT NOT NULL DEFAULT '1s'"},
		{"record_segment_duration", "TEXT NOT NULL DEFAULT '30s'"},
		{"record_delete_after", "TEXT NOT NULL DEFAULT '0s'"},
		{"record_max_part_size", "TEXT NOT NULL DEFAULT '50M'"},
	} {
		var exists int
		table := "global_setting"
		if column.name == "role" {
			table = "admin_user"
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('`+table+`') WHERE name=?`, column.name).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			if _, err := db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column.name + ` ` + column.definition); err != nil {
				return err
			}
		}
	}
	_, err = db.Exec(`INSERT INTO global_setting(id,brand_name,logo_text,logo_url,updated_at) VALUES(1,'brmlive 运维控制台','B','',?) ON CONFLICT(id) DO NOTHING`, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "time": time.Now().In(a.location).Format(time.RFC3339)})
}

func (a *app) branding(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	a.brandMu.RLock()
	brandName, logoText, logoURL := a.brandName, a.brandLogoText, a.brandLogoURL
	a.brandMu.RUnlock()
	if brandName == "" {
		brandName = "brmlive 运维控制台"
	}
	if logoText == "" {
		logoText = "B"
	}
	writeJSON(w, http.StatusOK, map[string]string{"brandName": brandName, "logoText": logoText, "logoUrl": logoURL})
}

func (a *app) loadBrandCache() {
	var name, text, logo string
	if err := a.db.QueryRow(`SELECT brand_name,logo_text,logo_url FROM global_setting WHERE id=1`).Scan(&name, &text, &logo); err != nil {
		name, text = "brmlive 运维控制台", "B"
	}
	a.brandMu.Lock()
	a.brandName, a.brandLogoText, a.brandLogoURL = name, text, logo
	a.brandMu.Unlock()
}

func (a *app) setBrandCache(name, text, logo string) {
	a.brandMu.Lock()
	a.brandName, a.brandLogoText, a.brandLogoURL = name, text, logo
	a.brandMu.Unlock()
}

// hlsProxy keeps HLS playlists and segments on the same origin as the
// console. The public reverse proxy only needs to forward port 8081.
func (a *app) hlsProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	rel := strings.TrimPrefix(r.URL.EscapedPath(), "/hls/")
	if rel == "" || strings.Contains(rel, "..") {
		http.Error(w, "invalid HLS path", http.StatusBadRequest)
		return
	}
	target, err := url.Parse(a.mediaHLSBase + "/" + rel)
	if err != nil {
		serverError(w, err)
		return
	}
	// Generated stream-access HLS URLs are public. The proxy retrieves the
	// stored read credential and uses it only for the internal MediaMTX request,
	// so neither playlists nor segment URLs expose stream-access secrets.
	parts := strings.Split(rel, "/")
	var access *streamAccessDB
	if len(parts) >= 2 {
		var d streamAccessDB
		var enabled int
		if err := a.db.QueryRow(`SELECT id,name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,expires_at,created_at,updated_at FROM stream_access WHERE slug=?`, parts[0]).Scan(&d.ID, &d.Name, &d.Slug, &d.PublishUser, &d.PublishCipher, &d.ReadUser, &d.ReadCipher, &enabled, &d.ExpiresAt, &d.CreatedAt, &d.UpdatedAt); err == nil {
			d.Enabled = enabled != 0
			access = &d
			if !access.Enabled || hasExpired(access.ExpiresAt) {
				unauthorized(w)
				return
			}
			hlsPass, openErr := a.crypt.open(access.ReadCipher)
			if openErr != nil {
				serverError(w, openErr)
				return
			}
			// MediaMTX puts the HLS session identifier in child playlist and
			// segment URLs. Preserve that opaque value across the internal proxy
			// request, but never forward legacy user/pass query credentials.
			query := r.URL.Query()
			query.Del("user")
			query.Del("pass")
			target.RawQuery = query.Encode()
			hlsUser := access.ReadUser
			request, reqErr := http.NewRequestWithContext(r.Context(), r.Method, target.String(), nil)
			if reqErr != nil {
				serverError(w, reqErr)
				return
			}
			request.SetBasicAuth(hlsUser, hlsPass)
			for _, key := range []string{"Range", "If-None-Match", "If-Modified-Since"} {
				if value := r.Header.Get(key); value != "" {
					request.Header.Set(key, value)
				}
			}
			resp, reqErr := a.mtx.client.Do(request)
			if reqErr != nil {
				http.Error(w, "HLS service unavailable", http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					if strings.HasSuffix(strings.ToLower(rel), ".m3u8") && strings.EqualFold(key, "Content-Length") {
						continue
					}
					w.Header().Add(key, value)
				}
			}
			if strings.HasSuffix(strings.ToLower(rel), ".m3u8") {
				w.Header().Del("Content-Length")
			}
			w.WriteHeader(resp.StatusCode)
			if r.Method == http.MethodGet {
				if strings.HasSuffix(strings.ToLower(rel), ".m3u8") && resp.StatusCode >= 200 && resp.StatusCode < 300 {
					body, readErr := io.ReadAll(resp.Body)
					if readErr == nil {
						_, _ = io.WriteString(w, string(body))
						return
					}
				}
				_, _ = io.Copy(w, resp.Body)
			}
			return
		}
	}
	target.RawQuery = r.URL.RawQuery
	request, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), nil)
	if err != nil {
		serverError(w, err)
		return
	}
	for _, key := range []string{"Range", "If-None-Match", "If-Modified-Since"} {
		if value := r.Header.Get(key); value != "" {
			request.Header.Set(key, value)
		}
	}
	resp, err := a.mtx.client.Do(request)
	if err != nil {
		http.Error(w, "HLS service unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for key, values := range resp.Header {
		for _, value := range values {
			if key == "Location" && strings.HasPrefix(value, "/") {
				value = "/hls" + value
			}
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodGet {
		_, _ = io.Copy(w, resp.Body)
	}
}

func (a *app) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var in struct{ Username, Password string }
	if !decode(r, &in) {
		badRequest(w, "invalid JSON")
		return
	}
	var hash, role string
	if err := a.db.QueryRow(`SELECT password_hash,role FROM admin_user WHERE username=?`, in.Username).Scan(&hash, &role); err != nil {
		unauthorized(w)
		return
	}
	role = normalizeRole(role)
	h := sha256.Sum256([]byte(in.Password))
	if hash != fmt.Sprintf("%x", h[:]) {
		unauthorized(w)
		return
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		serverError(w, err)
		return
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	ttl := a.sessionTTL()
	var expiresAt any
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UTC().Format(time.RFC3339)
	} else {
		expiresAt = nil
	}
	a.sessMu.Lock()
	if ttl > 0 {
		a.sessions[token] = time.Now().Add(ttl)
	} else {
		a.sessions[token] = time.Time{}
	}
	a.sessionUsers[token] = in.Username
	a.sessionRoles[token] = role
	a.sessMu.Unlock()
	var expiresDB any
	if ttl > 0 {
		expiresDB = time.Now().Add(ttl).UTC().Format(time.RFC3339)
	}
	if _, err := a.db.Exec(`INSERT INTO auth_session(token,username,role,expires_at) VALUES(?,?,?,?)`, token, in.Username, role, expiresDB); err != nil {
		log.Printf("persist auth session: %v", err)
	}
	writeJSON(w, 200, map[string]any{"token": token, "expiresAt": expiresAt, "role": role})
}

func (a *app) restoreSessions() {
	rows, err := a.db.Query(`SELECT token,username,role,expires_at FROM auth_session`)
	if err != nil {
		log.Printf("restore auth sessions: %v", err)
		return
	}
	defer rows.Close()
	now := time.Now().UTC()
	expiredTokens := []string{}
	for rows.Next() {
		var token, username, role string
		var expires sql.NullString
		if rows.Scan(&token, &username, &role, &expires) != nil {
			continue
		}
		var expiry time.Time
		if expires.Valid && expires.String != "" {
			parsed, parseErr := time.Parse(time.RFC3339, expires.String)
			if parseErr != nil || now.After(parsed) {
				expiredTokens = append(expiredTokens, token)
				continue
			}
			expiry = parsed
		}
		a.sessions[token] = expiry
		a.sessionUsers[token] = username
		a.sessionRoles[token] = normalizeRole(role)
	}
	rows.Close()
	for _, token := range expiredTokens {
		_, _ = a.db.Exec(`DELETE FROM auth_session WHERE token=?`, token)
	}
}

func (a *app) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			token = r.URL.Query().Get("access_token")
		}
		a.sessMu.RLock()
		expiry, ok := a.sessions[token]
		role := a.sessionRoles[token]
		a.sessMu.RUnlock()
		if !ok || token == "" || (!expiry.IsZero() && time.Now().After(expiry)) {
			if token != "" {
				_, _ = a.db.Exec(`DELETE FROM auth_session WHERE token=?`, token)
			}
			unauthorized(w)
			return
		}
		if role == "recording" && !recordingRouteAllowed(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "录制专用账号无权访问此功能"})
			return
		}
		next(w, r)
	}
}

func (a *app) logout(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	a.sessMu.Lock()
	delete(a.sessions, token)
	delete(a.sessionUsers, token)
	delete(a.sessionRoles, token)
	a.sessMu.Unlock()
	_, _ = a.db.Exec(`DELETE FROM auth_session WHERE token=?`, token)
	writeJSON(w, 200, map[string]bool{"ok": true})
}
func (a *app) me(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	a.sessMu.RLock()
	username := a.sessionUsers[token]
	role := a.sessionRoles[token]
	a.sessMu.RUnlock()
	writeJSON(w, 200, map[string]string{"username": username, "role": normalizeRole(role)})
}

func (a *app) changePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var in struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if !decode(r, &in) || len(in.NewPassword) < 8 || in.CurrentPassword == "" {
		badRequest(w, "currentPassword and a new password of at least 8 characters are required")
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	a.sessMu.RLock()
	username := a.sessionUsers[token]
	a.sessMu.RUnlock()
	if username == "" {
		unauthorized(w)
		return
	}
	var hash string
	if err := a.db.QueryRow(`SELECT password_hash FROM admin_user WHERE username=?`, username).Scan(&hash); err != nil {
		unauthorized(w)
		return
	}
	current := sha256.Sum256([]byte(in.CurrentPassword))
	if hash != fmt.Sprintf("%x", current[:]) {
		unauthorized(w)
		return
	}
	next := sha256.Sum256([]byte(in.NewPassword))
	if _, err := a.db.Exec(`UPDATE admin_user SET password_hash=? WHERE username=?`, fmt.Sprintf("%x", next[:]), username); err != nil {
		serverError(w, err)
		return
	}
	// Force all existing browser sessions to authenticate with the new credential.
	a.sessMu.Lock()
	a.sessions = map[string]time.Time{}
	a.sessionUsers = map[string]string{}
	a.sessionRoles = map[string]string{}
	a.sessMu.Unlock()
	_, _ = a.db.Exec(`DELETE FROM auth_session`)
	a.event("info", "password_changed", username)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (a *app) users(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rows, err := a.db.Query(`SELECT username,role FROM admin_user ORDER BY username`)
		if err != nil {
			serverError(w, err)
			return
		}
		out := []map[string]string{}
		for rows.Next() {
			var username, role string
			if rows.Scan(&username, &role) == nil {
				out = append(out, map[string]string{"username": username, "role": normalizeRole(role)})
			}
		}
		rows.Close()
		writeJSON(w, 200, out)
	case http.MethodPost:
		var in struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if !decode(r, &in) || !validUsername(in.Username) || len(in.Password) < 8 {
			badRequest(w, "username must be 1-64 letters, numbers, _ or - and password must be at least 8 characters")
			return
		}
		in.Role = normalizeRole(in.Role)
		hash := sha256.Sum256([]byte(in.Password))
		if _, err := a.db.Exec(`INSERT INTO admin_user(username,password_hash,role) VALUES(?,?,?)`, in.Username, fmt.Sprintf("%x", hash[:]), in.Role); err != nil {
			conflictOrServer(w, err)
			return
		}
		a.event("info", "user_created", in.Username)
		writeJSON(w, http.StatusCreated, map[string]string{"username": in.Username, "role": in.Role})
	case http.MethodPatch:
		username := strings.TrimSpace(r.URL.Query().Get("username"))
		var in struct {
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if !validUsername(username) || !decode(r, &in) || (in.Password != "" && len(in.Password) < 8) {
			badRequest(w, "username and a password of at least 8 characters are required")
			return
		}
		if strings.TrimSpace(in.Role) == "" {
			if err := a.db.QueryRow(`SELECT role FROM admin_user WHERE username=?`, username).Scan(&in.Role); err != nil {
				notFoundOrServer(w, err)
				return
			}
		}
		in.Role = normalizeRole(in.Role)
		var result sql.Result
		var err error
		if in.Password == "" {
			result, err = a.db.Exec(`UPDATE admin_user SET role=? WHERE username=?`, in.Role, username)
		} else {
			hash := sha256.Sum256([]byte(in.Password))
			result, err = a.db.Exec(`UPDATE admin_user SET password_hash=?,role=? WHERE username=?`, fmt.Sprintf("%x", hash[:]), in.Role, username)
		}
		if err != nil {
			serverError(w, err)
			return
		}
		if n, _ := result.RowsAffected(); n == 0 {
			notFoundOrServer(w, sql.ErrNoRows)
			return
		}
		role := normalizeRole(in.Role)
		a.sessMu.Lock()
		for token, sessionUser := range a.sessionUsers {
			if sessionUser == username {
				a.sessionRoles[token] = role
			}
		}
		a.sessMu.Unlock()
		_, _ = a.db.Exec(`UPDATE auth_session SET role=? WHERE username=?`, role, username)
		a.event("info", "user_password_changed", username)
		writeJSON(w, 200, map[string]bool{"ok": true})
	case http.MethodDelete:
		username := strings.TrimSpace(r.URL.Query().Get("username"))
		if !validUsername(username) {
			badRequest(w, "valid username is required")
			return
		}
		var count int
		if err := a.db.QueryRow(`SELECT COUNT(*) FROM admin_user`).Scan(&count); err != nil {
			serverError(w, err)
			return
		}
		if count <= 1 {
			badRequest(w, "至少保留一个账号")
			return
		}
		if _, err := a.db.Exec(`DELETE FROM admin_user WHERE username=?`, username); err != nil {
			serverError(w, err)
			return
		}
		_, _ = a.db.Exec(`DELETE FROM auth_session WHERE username=?`, username)
		a.sessMu.Lock()
		for token, sessionUser := range a.sessionUsers {
			if sessionUser == username {
				delete(a.sessions, token)
				delete(a.sessionUsers, token)
				delete(a.sessionRoles, token)
			}
		}
		a.sessMu.Unlock()
		a.event("info", "user_deleted", username)
		writeJSON(w, 200, map[string]bool{"ok": true})
	default:
		methodNotAllowed(w)
	}
}

func validUsername(username string) bool {
	if len(username) < 1 || len(username) > 64 {
		return false
	}
	for _, r := range username {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func normalizeRole(role string) string {
	if strings.EqualFold(strings.TrimSpace(role), "recording") {
		return "recording"
	}
	return "admin"
}

func recordingRouteAllowed(r *http.Request) bool {
	path := r.URL.Path
	if path == "/api/v1/auth/me" && r.Method == http.MethodGet {
		return true
	}
	if path == "/api/v1/auth/logout" && r.Method == http.MethodPost {
		return true
	}
	if path == "/api/v1/recordings" && r.Method == http.MethodGet {
		return true
	}
	if strings.HasPrefix(path, "/api/v1/recordings/") && (r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete) {
		return true
	}
	return path == "/api/v1/executions" && r.Method == http.MethodGet
}

type globalSettings struct {
	BrandName             string `json:"brandName"`
	LogoText              string `json:"logoText"`
	LogoURL               string `json:"logoUrl"`
	SessionTTLMinutes     int    `json:"sessionTtlMinutes"`
	RecordFormat          string `json:"recordFormat"`
	RecordBitrateKbps     int    `json:"recordBitrateKbps"`
	RecordDuration        string `json:"recordDuration"`
	RecordPartDuration    string `json:"recordPartDuration"`
	RecordSegmentDuration string `json:"recordSegmentDuration"`
	RecordDeleteAfter     string `json:"recordDeleteAfter"`
	RecordMaxPartSize     string `json:"recordMaxPartSize"`
}

func (a *app) sessionTTL() time.Duration {
	var minutes int
	if err := a.db.QueryRow(`SELECT session_ttl_minutes FROM global_setting WHERE id=1`).Scan(&minutes); err != nil || minutes <= 0 {
		return 0
	}
	return time.Duration(minutes) * time.Minute
}

func (a *app) settings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var s globalSettings
		if err := a.db.QueryRow(`SELECT brand_name,logo_text,logo_url,session_ttl_minutes,record_format,record_bitrate_kbps,record_duration,record_part_duration,record_segment_duration,record_delete_after,record_max_part_size FROM global_setting WHERE id=1`).Scan(&s.BrandName, &s.LogoText, &s.LogoURL, &s.SessionTTLMinutes, &s.RecordFormat, &s.RecordBitrateKbps, &s.RecordDuration, &s.RecordPartDuration, &s.RecordSegmentDuration, &s.RecordDeleteAfter, &s.RecordMaxPartSize); err != nil {
			serverError(w, err)
			return
		}
		if s.RecordFormat == "fmp4" || s.RecordFormat == "" {
			s.RecordFormat = "mp4-h264"
		}
		if s.RecordDuration == "" {
			s.RecordDuration = s.RecordSegmentDuration
		}
		if s.RecordDuration == "0s" {
			s.RecordDuration = "0"
		}
		writeJSON(w, 200, s)
	case http.MethodPatch:
		var in globalSettings
		if !decode(r, &in) {
			badRequest(w, "invalid JSON")
			return
		}
		in.LogoText = strings.TrimSpace(in.LogoText)
		in.LogoURL = strings.TrimSpace(in.LogoURL)
		in.BrandName = strings.TrimSpace(in.BrandName)
		if in.BrandName == "" {
			in.BrandName = "brmlive 运维控制台"
		}
		if len([]rune(in.BrandName)) > 64 {
			badRequest(w, "brandName must be 64 characters or fewer")
			return
		}
		if len([]rune(in.LogoText)) > 32 {
			badRequest(w, "logoText must be 32 characters or fewer")
			return
		}
		if len(in.LogoURL) > 2048 {
			badRequest(w, "logoUrl is too long")
			return
		}
		if in.SessionTTLMinutes < 0 || in.SessionTTLMinutes > 525600 {
			badRequest(w, "sessionTtlMinutes must be 0-525600")
			return
		}
		in.RecordFormat = strings.ToLower(strings.TrimSpace(in.RecordFormat))
		if in.RecordFormat == "mp4-h264" {
			in.RecordFormat = "fmp4"
		}
		if in.RecordFormat != "fmp4" && in.RecordFormat != "mpegts" {
			badRequest(w, "recordFormat must be mp4-h264 or mpegts")
			return
		}
		if in.RecordBitrateKbps < 0 || in.RecordBitrateKbps > 200000 {
			badRequest(w, "recordBitrateKbps must be 0-200000")
			return
		}
		if strings.TrimSpace(in.RecordDuration) == "" || len(in.RecordDuration) > 32 || !regexp.MustCompile(`^(0|[0-9]+(ms|s|m|h|d))$`).MatchString(in.RecordDuration) {
			badRequest(w, "recordDuration must be 0 or a duration such as 30m or 1h")
			return
		}
		if in.RecordDuration == "0" {
			in.RecordDuration = "0s"
		}
		in.RecordSegmentDuration = in.RecordDuration
		if in.RecordDuration == "0s" {
			in.RecordSegmentDuration = "24h"
		}
		in.RecordPartDuration, in.RecordDeleteAfter, in.RecordMaxPartSize = "1s", "0s", "50M"
		if _, err := a.db.Exec(`UPDATE global_setting SET brand_name=?,logo_text=?,logo_url=?,session_ttl_minutes=?,record_format=?,record_bitrate_kbps=?,record_duration=?,record_part_duration=?,record_segment_duration=?,record_delete_after=?,record_max_part_size=?,updated_at=? WHERE id=1`, in.BrandName, in.LogoText, in.LogoURL, in.SessionTTLMinutes, in.RecordFormat, in.RecordBitrateKbps, in.RecordDuration, in.RecordPartDuration, in.RecordSegmentDuration, in.RecordDeleteAfter, in.RecordMaxPartSize, time.Now().UTC().Format(time.RFC3339)); err != nil {
			serverError(w, err)
			return
		}
		a.setBrandCache(in.BrandName, in.LogoText, in.LogoURL)
		if err := a.syncAllPrograms(r.Context()); err != nil {
			a.event("error", "mediamtx_sync", err.Error())
			badRequest(w, "设置已保存，但 MediaMTX 配置同步失败: "+err.Error())
			return
		}
		a.event("info", "settings_updated", "全局设置已更新")
		out := in
		if out.RecordDuration == "0s" {
			out.RecordDuration = "0"
		}
		writeJSON(w, 200, out)
	default:
		methodNotAllowed(w)
	}
}

func (a *app) streamAccesses(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rows, err := a.db.Query(`SELECT id,name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,expires_at,created_at,updated_at FROM stream_access ORDER BY id DESC`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer rows.Close()
		out := []streamAccess{}
		for rows.Next() {
			var d streamAccessDB
			var enabled int
			if err := rows.Scan(&d.ID, &d.Name, &d.Slug, &d.PublishUser, &d.PublishCipher, &d.ReadUser, &d.ReadCipher, &enabled, &d.ExpiresAt, &d.CreatedAt, &d.UpdatedAt); err != nil {
				serverError(w, err)
				return
			}
			d.Enabled = enabled != 0
			out = append(out, a.streamAccessView(d, false))
		}
		writeJSON(w, 200, out)
	case http.MethodPost:
		var in streamAccessInput
		if !decode(r, &in) || strings.TrimSpace(in.Name) == "" {
			badRequest(w, "name is required")
			return
		}
		in.Name = strings.TrimSpace(in.Name)
		if in.Slug == "" {
			in.Slug = "stream-" + randomToken(6)
		}
		if !validSlug(in.Slug) {
			badRequest(w, "slug must contain letters, numbers, _ or -")
			return
		}
		var programID int64
		if err := a.db.QueryRow(`SELECT id FROM program WHERE slug=?`, in.Slug).Scan(&programID); err == nil {
			conflictOrServer(w, errors.New("unique slug conflicts with an existing program"))
			return
		}
		expires, err := validateExpiry(in.ExpiresAt)
		if err != nil {
			badRequest(w, err.Error())
			return
		}
		enabled := true
		if in.Enabled != nil {
			enabled = *in.Enabled
		}
		pubUser, readUser := "pub_"+randomToken(5), "read_"+randomToken(5)
		pubPass, readPass := randomToken(24), randomToken(24)
		pubCipher, err := a.crypt.seal(pubPass)
		if err != nil {
			serverError(w, err)
			return
		}
		readCipher, err := a.crypt.seal(readPass)
		if err != nil {
			serverError(w, err)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		_, err = a.db.Exec(`INSERT INTO stream_access(name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, in.Name, in.Slug, pubUser, pubCipher, readUser, readCipher, boolInt(enabled), nullableString(sql.NullString{String: expires, Valid: expires != ""}), now, now)
		if err != nil {
			conflictOrServer(w, err)
			return
		}
		id, _ := lastInsertID(a.db)
		if err := a.syncStreamAccess(r.Context(), id); err != nil {
			a.event("error", "stream_access_sync", err.Error())
			serverError(w, err)
			return
		}
		var d streamAccessDB
		if err := a.loadStreamAccess(id, &d); err != nil {
			serverError(w, err)
			return
		}
		view := a.streamAccessView(d, true)
		view.PublishURL, view.PullURL, view.HLSURL = a.streamAccessURLs(d, pubPass, readPass)
		writeJSON(w, http.StatusCreated, view)
	default:
		methodNotAllowed(w)
	}
}

func (a *app) streamAccessSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.Path, "/api/v1/stream-access/")
	if len(parts) < 1 || len(parts) > 2 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		badRequest(w, "invalid stream access id")
		return
	}
	var d streamAccessDB
	if err := a.loadStreamAccess(id, &d); err != nil {
		notFoundOrServer(w, err)
		return
	}
	if len(parts) == 2 {
		if parts[1] != "rotate" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		pubPass, readPass := randomToken(24), randomToken(24)
		pubCipher, sealErr := a.crypt.seal(pubPass)
		if sealErr != nil {
			serverError(w, sealErr)
			return
		}
		readCipher, sealErr := a.crypt.seal(readPass)
		if sealErr != nil {
			serverError(w, sealErr)
			return
		}
		d.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if _, err := a.db.Exec(`UPDATE stream_access SET publish_pass_cipher=?,read_pass_cipher=?,updated_at=? WHERE id=?`, pubCipher, readCipher, d.UpdatedAt, id); err != nil {
			serverError(w, err)
			return
		}
		if err := a.syncStreamAccess(r.Context(), id); err != nil {
			serverError(w, err)
			return
		}
		view := a.streamAccessView(d, true)
		view.PublishURL, view.PullURL, view.HLSURL = a.streamAccessURLs(d, pubPass, readPass)
		writeJSON(w, 200, view)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, a.streamAccessView(d, false))
	case http.MethodPatch:
		var in streamAccessInput
		if !decode(r, &in) {
			badRequest(w, "invalid JSON")
			return
		}
		if strings.TrimSpace(in.Name) != "" {
			d.Name = strings.TrimSpace(in.Name)
		}
		if strings.TrimSpace(in.Slug) != "" {
			if !validSlug(in.Slug) {
				badRequest(w, "invalid slug")
				return
			}
			candidate := strings.TrimSpace(in.Slug)
			var programID int64
			if err := a.db.QueryRow(`SELECT id FROM program WHERE slug=?`, candidate).Scan(&programID); err == nil {
				conflictOrServer(w, errors.New("unique slug conflicts with an existing program"))
				return
			}
			d.Slug = candidate
		}
		if in.Enabled != nil {
			d.Enabled = *in.Enabled
		}
		if in.ExpiresAt != "" {
			exp, e := validateExpiry(in.ExpiresAt)
			if e != nil {
				badRequest(w, e.Error())
				return
			}
			d.ExpiresAt = sql.NullString{String: exp, Valid: exp != ""}
		}
		d.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if _, err := a.db.Exec(`UPDATE stream_access SET name=?,slug=?,enabled=?,expires_at=?,updated_at=? WHERE id=?`, d.Name, d.Slug, boolInt(d.Enabled), nullableString(d.ExpiresAt), d.UpdatedAt, id); err != nil {
			conflictOrServer(w, err)
			return
		}
		if err := a.syncStreamAccess(r.Context(), id); err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 200, a.streamAccessView(d, false))
	case http.MethodDelete:
		if _, err := a.db.Exec(`DELETE FROM stream_access WHERE id=?`, id); err != nil {
			serverError(w, err)
			return
		}
		_ = a.mtx.deletePath(r.Context(), d.Slug)
		writeJSON(w, 200, map[string]bool{"ok": true})
	default:
		methodNotAllowed(w)
	}
}

func (a *app) loadStreamAccess(id int64, d *streamAccessDB) error {
	var enabled int
	err := a.db.QueryRow(`SELECT id,name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,expires_at,created_at,updated_at FROM stream_access WHERE id=?`, id).Scan(&d.ID, &d.Name, &d.Slug, &d.PublishUser, &d.PublishCipher, &d.ReadUser, &d.ReadCipher, &enabled, &d.ExpiresAt, &d.CreatedAt, &d.UpdatedAt)
	d.Enabled = enabled != 0
	return err
}

func (a *app) streamAccessView(d streamAccessDB, includeURL bool) streamAccess {
	status, readers := "offline", 0
	if !d.Enabled {
		status = "disabled"
	} else if hasExpired(d.ExpiresAt) {
		status = "expired"
	} else {
		status = "waiting"
	}
	if a.mtx != nil {
		if paths, err := a.mtx.paths(context.Background()); err == nil {
			for _, p := range paths {
				if p.Name == d.Slug {
					if p.Ready {
						status = "online"
					}
					readers = len(p.Readers)
				}
			}
		}
	}
	return streamAccess{ID: d.ID, Name: d.Name, Slug: d.Slug, Enabled: d.Enabled, ExpiresAt: d.ExpiresAt.String, PublishURLHint: "rtmp://<host>:1935/" + d.Slug, PullURLHint: "rtmp://<host>:1935/" + d.Slug, HLSURLHint: "http://<host>:8080/hls/" + d.Slug + "/index.m3u8", Status: status, Readers: readers, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
}

func (a *app) streamAccessURLs(d streamAccessDB, pubPass, readPass string) (string, string, string) {
	base := strings.TrimRight(env("PUBLIC_RTMP_BASE_URL", "rtmp://localhost:1935"), "/")
	pub := base + "/" + d.Slug + "?user=" + url.QueryEscape(d.PublishUser) + "&pass=" + url.QueryEscape(pubPass)
	pull := base + "/" + d.Slug + "?user=" + url.QueryEscape(d.ReadUser) + "&pass=" + url.QueryEscape(readPass)
	hlsBase := strings.TrimRight(env("PUBLIC_HLS_BASE_URL", "http://localhost:8080/hls"), "/")
	hls := hlsBase + "/" + url.PathEscape(d.Slug) + "/index.m3u8"
	return pub, pull, hls
}

func (a *app) syncStreamAccess(ctx context.Context, id int64) error {
	var d streamAccessDB
	if err := a.loadStreamAccess(id, &d); err != nil {
		return err
	}
	if !d.Enabled || hasExpired(d.ExpiresAt) {
		// Avoid issuing a DELETE for a path that is already absent. Besides
		// being unnecessary, MediaMTX logs every such 404 as an API error.
		paths, err := a.mtx.paths(ctx)
		if err != nil {
			return err
		}
		for _, p := range paths {
			if p.Name == d.Slug {
				return a.mtx.deletePath(ctx, d.Slug)
			}
		}
		return nil
	}
	return a.mtx.upsertPath(ctx, d.Slug, map[string]any{"source": "publisher", "sourceOnDemand": false})
}

func (a *app) streamAccessMonitor(ctx context.Context) {
	a.syncAllStreamAccess(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.syncAllStreamAccess(ctx)
		}
	}
}

// mediaMTXMonitor repairs dynamic program paths after a MediaMTX restart.
// MediaMTX does not persist paths created through its API, while SQLite does.
func (a *app) mediaMTXMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.reconcileMediaMTX(ctx)
		}
	}
}

func (a *app) reconcileMediaMTX(ctx context.Context) {
	paths, err := a.mtx.paths(ctx)
	if err != nil {
		return
	}
	present := make(map[string]mediaPath, len(paths))
	for _, p := range paths {
		present[p.Name] = p
	}
	rows, err := a.db.Query(`SELECT id,slug,source_url FROM program WHERE enabled=1`)
	if err != nil {
		return
	}
	// SQLite is deliberately configured with one connection for the Windows
	// bind-mounted local deployment. Do not call syncProgram or syncAllPrograms
	// while this result set is open: both need that same connection and would
	// block the control API indefinitely.
	type mediaProgram struct {
		id        int64
		slug      string
		sourceURL string
	}
	programs := []mediaProgram{}
	for rows.Next() {
		var p mediaProgram
		if err := rows.Scan(&p.id, &p.slug, &p.sourceURL); err == nil {
			programs = append(programs, p)
		}
	}
	rows.Close()
	missing := false
	for _, program := range programs {
		if _, ok := present[program.slug]; !ok {
			missing = true
			continue
		}
		p := present[program.slug]
		if isPublisherSource(program.sourceURL) || !isStaleMediaPath(p) {
			a.clearMediaPathHealth(program.slug)
			continue
		}
		if a.shouldRecoverMediaPath(program.slug, time.Now()) {
			if err := a.syncProgram(ctx, program.id); err != nil {
				a.event("warn", "mediamtx_recover", fmt.Sprintf("%s: %v", program.slug, err))
				continue
			}
			a.restoreRecordingState(ctx, program.id, program.slug)
			a.event("warn", "mediamtx_recover", fmt.Sprintf("%s HLS 源长期未就绪，已自动重建 MediaMTX 路径", program.slug))
		}
	}
	if missing {
		if err := a.syncAllPrograms(ctx); err != nil {
			a.event("warn", "mediamtx_reconcile", err.Error())
		}
	}
}

func isStaleMediaPath(p mediaPath) bool {
	return !p.Ready && !p.Available && len(p.Tracks) == 0 && p.InboundBytes == 0
}

func (a *app) shouldRecoverMediaPath(slug string, now time.Time) bool {
	a.mediaHealthMu.Lock()
	defer a.mediaHealthMu.Unlock()
	if a.mediaHealth == nil {
		a.mediaHealth = map[string]mediaPathHealth{}
	}
	h := a.mediaHealth[slug]
	if h.staleSince.IsZero() {
		h.staleSince = now
		a.mediaHealth[slug] = h
		return false
	}
	if now.Sub(h.staleSince) < mediaPathStaleAfter || (!h.lastRestart.IsZero() && now.Sub(h.lastRestart) < mediaPathRetryAfter) {
		return false
	}
	h.lastRestart = now
	a.mediaHealth[slug] = h
	return true
}

func (a *app) clearMediaPathHealth(slug string) {
	a.mediaHealthMu.Lock()
	if a.mediaHealth != nil {
		delete(a.mediaHealth, slug)
	}
	a.mediaHealthMu.Unlock()
}

func (a *app) restoreRecordingState(ctx context.Context, id int64, slug string) {
	a.stateMu.Lock()
	active := a.active[id]
	a.stateMu.Unlock()
	if active {
		if err := a.mtx.patchRecord(ctx, slug, true); err != nil {
			a.event("warn", "mediamtx_recover", fmt.Sprintf("%s 录制状态恢复失败: %v", slug, err))
		}
	}
}

func (a *app) syncAllStreamAccess(ctx context.Context) {
	rows, err := a.db.Query(`SELECT id FROM stream_access ORDER BY id`)
	if err != nil {
		return
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		if err := a.syncStreamAccess(ctx, id); err != nil {
			a.event("warn", "stream_access_sync", fmt.Sprintf("通道 %d: %v", id, err))
		}
	}
}

func validateExpiry(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil || !t.After(time.Now()) {
		return "", errors.New("expiresAt must be a future RFC3339 timestamp")
	}
	return t.UTC().Format(time.RFC3339), nil
}
func parseExpiry(raw string) time.Time { t, _ := time.Parse(time.RFC3339, raw); return t }
func hasExpired(v sql.NullString) bool {
	return v.Valid && strings.TrimSpace(v.String) != "" && !parseExpiry(v.String).After(time.Now())
}
func nullableString(v sql.NullString) any {
	if !v.Valid || v.String == "" {
		return nil
	}
	return v.String
}
func randomToken(n int) string {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:n]
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
func lastInsertID(db *sql.DB) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT last_insert_rowid()").Scan(&id)
	return id, err
}

func (a *app) programs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rows, err := a.db.Query(`SELECT id,name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at FROM program ORDER BY id`)
		if err != nil {
			serverError(w, err)
			return
		}
		defer rows.Close()
		out := []program{}
		for rows.Next() {
			var p program
			var enabled, record int
			if err := rows.Scan(&p.ID, &p.Name, &p.Slug, &p.Kind, &p.SourceURL, &enabled, &record, &p.CreatedAt, &p.UpdatedAt); err != nil {
				serverError(w, err)
				return
			}
			p.Enabled = enabled != 0
			p.RecordEnabled = record != 0
			out = append(out, p)
		}
		writeJSON(w, 200, out)
	case http.MethodPost:
		var in programInput
		if !decode(r, &in) || strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.Slug) == "" || strings.TrimSpace(in.SourceURL) == "" {
			badRequest(w, "name, slug and sourceUrl are required")
			return
		}
		in.Name, in.Slug, in.SourceURL = strings.TrimSpace(in.Name), strings.TrimSpace(in.Slug), strings.TrimSpace(in.SourceURL)
		if !validSlug(in.Slug) {
			badRequest(w, "slug must contain letters, numbers, _ or -")
			return
		}
		if !validSourceURL(in.SourceURL) {
			badRequest(w, "sourceUrl must use a supported media input protocol")
			return
		}
		now := time.Now().UTC().Format(time.RFC3339)
		enabled, record := true, false
		if in.Enabled != nil {
			enabled = *in.Enabled
		}
		if in.RecordEnabled != nil {
			record = *in.RecordEnabled
		}
		res, err := a.db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`, in.Name, in.Slug, defaultKind(in.Kind), in.SourceURL, boolInt(enabled), boolInt(record), now, now)
		if err != nil {
			conflictOrServer(w, err)
			return
		}
		id, _ := res.LastInsertId()
		if err := a.syncProgram(r.Context(), id); err != nil {
			a.event("error", "mediamtx_sync", err.Error())
			// The database write succeeded. Return the created program so the UI
			// does not report a false failure; the sync error is visible in events.
			a.event("warn", "mediamtx_sync", "节目已保存，但 MediaMTX 同步待重试: "+err.Error())
		}
		p, err := a.getProgram(id)
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 201, p)
	default:
		methodNotAllowed(w)
	}
}

func (a *app) programSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.Path, "/api/v1/programs/")
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		badRequest(w, "invalid program id")
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			p, err := a.getProgram(id)
			if err != nil {
				notFoundOrServer(w, err)
				return
			}
			writeJSON(w, 200, p)
		case http.MethodPatch:
			a.updateProgram(w, r, id)
		case http.MethodDelete:
			p, getErr := a.getProgram(id)
			if getErr != nil {
				notFoundOrServer(w, getErr)
				return
			}
			if _, err := a.db.Exec(`DELETE FROM program WHERE id=?`, id); err != nil {
				serverError(w, err)
				return
			}
			if err := a.mtx.deletePath(r.Context(), p.Slug); err != nil {
				a.event("warn", "mediamtx_cleanup", err.Error())
			}
			writeJSON(w, 200, map[string]bool{"ok": true})
		default:
			methodNotAllowed(w)
		}
		return
	}
	switch parts[1] {
	case "destinations":
		a.programDestinations(w, r, id)
	case "schedules":
		a.programSchedules(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func (a *app) updateProgram(w http.ResponseWriter, r *http.Request, id int64) {
	var in programInput
	if !decode(r, &in) {
		badRequest(w, "invalid JSON")
		return
	}
	p, err := a.getProgram(id)
	if err != nil {
		notFoundOrServer(w, err)
		return
	}
	if in.Name != "" {
		p.Name = in.Name
	}
	if in.Slug != "" {
		if !validSlug(in.Slug) {
			badRequest(w, "invalid slug")
			return
		}
		p.Slug = in.Slug
	}
	if in.Kind != "" {
		p.Kind = in.Kind
	}
	if in.SourceURL != "" {
		in.SourceURL = strings.TrimSpace(in.SourceURL)
		if !validSourceURL(in.SourceURL) {
			badRequest(w, "sourceUrl must use a supported media input protocol")
			return
		}
		p.SourceURL = in.SourceURL
	}
	if in.Enabled != nil {
		p.Enabled = *in.Enabled
	}
	if in.RecordEnabled != nil {
		p.RecordEnabled = *in.RecordEnabled
	}
	p.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	_, err = a.db.Exec(`UPDATE program SET name=?,slug=?,kind=?,source_url=?,enabled=?,record_enabled=?,updated_at=? WHERE id=?`, p.Name, p.Slug, p.Kind, p.SourceURL, boolInt(p.Enabled), boolInt(p.RecordEnabled), p.UpdatedAt, id)
	if err != nil {
		conflictOrServer(w, err)
		return
	}
	if err = a.syncProgram(r.Context(), id); err != nil {
		a.event("warn", "mediamtx_sync", "节目已更新，但 MediaMTX 同步待重试: "+err.Error())
	}
	writeJSON(w, 200, p)
}

func (a *app) destinations(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		rows, err := a.db.Query(`SELECT id,name,url,protocol,stream_key_cipher,enabled FROM destination ORDER BY id`)
		if err != nil {
			serverError(w, err)
			return
		}
		out := []destination{}
		for rows.Next() {
			var d destination
			var cipherText string
			var enabled int
			if err := rows.Scan(&d.ID, &d.Name, &d.URL, &d.Protocol, &cipherText, &enabled); err != nil {
				serverError(w, err)
				return
			}
			d.Enabled = enabled != 0
			d.KeyMasked = maskKey(cipherText)
			out = append(out, d)
		}
		rows.Close()
		a.populateDestinationStatuses(r.Context(), out)
		writeJSON(w, 200, out)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var in destinationInput
	if !decode(r, &in) || strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.URL) == "" {
		badRequest(w, "name and url are required")
		return
	}
	if !validForwardURL(in.URL) {
		badRequest(w, "推流地址必须使用 rtmp:// 或 rtmps://")
		return
	}
	if in.Protocol != "" && !forwardProtocolMatches(in.URL, in.Protocol) {
		badRequest(w, "协议与推流地址前缀不一致")
		return
	}
	in.URL = strings.TrimRight(strings.TrimSpace(in.URL), "/")
	in.StreamKey = strings.TrimSpace(in.StreamKey)
	if strings.ContainsAny(in.StreamKey, "#\r\n\t ") {
		badRequest(w, "Stream Key 不能包含空格、# 或换行")
		return
	}
	cipherText, err := a.crypt.seal(in.StreamKey)
	if err != nil {
		serverError(w, err)
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := a.db.Exec(`INSERT INTO destination(name,url,protocol,stream_key_cipher,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, in.Name, in.URL, defaultProtocol(in.Protocol), cipherText, boolInt(enabled), now, now)
	if err != nil {
		conflictOrServer(w, err)
		return
	}
	id, _ := res.LastInsertId()
	d, _ := a.getDestination(id)
	if err := a.syncProgramsForDestination(r.Context(), id); err != nil {
		a.event("warn", "mediamtx_sync", fmt.Sprintf("目标 %s 已保存，但绑定节目同步失败: %v", d.Name, err))
	}
	writeJSON(w, 201, d)
}

// syncProgramsForDestination refreshes every MediaMTX path that references a
// destination. Destination edits must take effect immediately; otherwise the
// old URL/key remains in MediaMTX until a program is edited manually.
func (a *app) syncProgramsForDestination(ctx context.Context, destinationID int64) error {
	rows, err := a.db.Query(`SELECT program_id FROM program_destination WHERE destination_id=?`, destinationID)
	if err != nil {
		return err
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	var firstErr error
	for _, id := range ids {
		if err := a.syncProgramWithRetry(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (a *app) syncProgramWithRetry(ctx context.Context, id int64) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := a.syncProgram(ctx, id); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func (a *app) destinationSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.Path, "/api/v1/destinations/")
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		badRequest(w, "invalid destination id")
		return
	}
	if r.Method == http.MethodGet {
		d, err := a.getDestination(id)
		if err != nil {
			notFoundOrServer(w, err)
			return
		}
		writeJSON(w, 200, d)
		return
	}
	if r.Method == http.MethodDelete {
		rows, queryErr := a.db.Query(`SELECT program_id FROM program_destination WHERE destination_id=?`, id)
		if queryErr != nil {
			serverError(w, queryErr)
			return
		}
		programIDs := []int64{}
		for rows.Next() {
			var programID int64
			if rows.Scan(&programID) == nil {
				programIDs = append(programIDs, programID)
			}
		}
		rows.Close()
		_, err = a.db.Exec(`DELETE FROM destination WHERE id=?`, id)
		if err != nil {
			serverError(w, err)
			return
		}
		for _, programID := range programIDs {
			if syncErr := a.syncProgramWithRetry(r.Context(), programID); syncErr != nil {
				a.event("warn", "mediamtx_sync", fmt.Sprintf("删除目标 %d 后节目 %d 同步失败: %v", id, programID, syncErr))
			}
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.Method != http.MethodPatch {
		methodNotAllowed(w)
		return
	}
	var in destinationInput
	if !decode(r, &in) {
		badRequest(w, "invalid JSON")
		return
	}
	d, err := a.getDestination(id)
	if err != nil {
		notFoundOrServer(w, err)
		return
	}
	if in.Name != "" {
		d.Name = in.Name
	}
	if in.URL != "" {
		in.URL = strings.TrimRight(strings.TrimSpace(in.URL), "/")
		if !validForwardURL(in.URL) {
			badRequest(w, "推流地址必须使用 rtmp:// 或 rtmps://")
			return
		}
		if in.Protocol != "" && !forwardProtocolMatches(in.URL, in.Protocol) {
			badRequest(w, "协议与推流地址前缀不一致")
			return
		}
		d.URL = in.URL
	}
	if in.Protocol != "" {
		if !forwardProtocolMatches(d.URL, in.Protocol) {
			badRequest(w, "协议与推流地址前缀不一致")
			return
		}
		d.Protocol = in.Protocol
	}
	if in.Enabled != nil {
		d.Enabled = *in.Enabled
	}
	cipherText := ""
	if in.ClearStreamKey {
		cipherText = ""
	} else if in.StreamKey != "" {
		in.StreamKey = strings.TrimSpace(in.StreamKey)
		if strings.ContainsAny(in.StreamKey, "#\r\n\t ") {
			badRequest(w, "Stream Key 不能包含空格、# 或换行")
			return
		}
		cipherText, err = a.crypt.seal(in.StreamKey)
		if err != nil {
			serverError(w, err)
			return
		}
	} else {
		a.db.QueryRow(`SELECT stream_key_cipher FROM destination WHERE id=?`, id).Scan(&cipherText)
	}
	_, err = a.db.Exec(`UPDATE destination SET name=?,url=?,protocol=?,stream_key_cipher=?,enabled=?,updated_at=? WHERE id=?`, d.Name, d.URL, d.Protocol, cipherText, boolInt(d.Enabled), time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		serverError(w, err)
		return
	}
	if syncErr := a.syncProgramsForDestination(r.Context(), id); syncErr != nil {
		a.event("warn", "mediamtx_sync", fmt.Sprintf("目标 %s 已更新，但绑定节目同步失败: %v", d.Name, syncErr))
	}
	writeJSON(w, 200, d)
}

func (a *app) programDestinations(w http.ResponseWriter, r *http.Request, programID int64) {
	if r.Method == http.MethodPatch {
		parts := pathParts(r.URL.Path, fmt.Sprintf("/api/v1/programs/%d/destinations/", programID))
		if len(parts) != 1 {
			http.NotFound(w, r)
			return
		}
		destinationID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			badRequest(w, "invalid destination id")
			return
		}
		var in struct {
			Enabled *bool `json:"enabled"`
		}
		if !decode(r, &in) || in.Enabled == nil {
			badRequest(w, "enabled is required")
			return
		}
		if _, err := a.db.Exec(`UPDATE program_destination SET enabled=? WHERE program_id=? AND destination_id=?`, boolInt(*in.Enabled), programID, destinationID); err != nil {
			serverError(w, err)
			return
		}
		if err := a.syncProgramWithRetry(r.Context(), programID); err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "enabled": *in.Enabled})
		return
	}
	if r.Method == http.MethodGet {
		rows, err := a.db.Query(`SELECT d.id,d.name,d.url,d.protocol,d.stream_key_cipher,d.enabled FROM destination d JOIN program_destination pd ON pd.destination_id=d.id WHERE pd.program_id=? ORDER BY d.id`, programID)
		if err != nil {
			serverError(w, err)
			return
		}
		defer rows.Close()
		out := []destination{}
		for rows.Next() {
			var d destination
			var c string
			var e int
			rows.Scan(&d.ID, &d.Name, &d.URL, &d.Protocol, &c, &e)
			d.Enabled = e != 0
			d.KeyMasked = maskKey(c)
			out = append(out, d)
		}
		writeJSON(w, 200, out)
		return
	}
	if r.Method == http.MethodPost {
		var in struct {
			DestinationID int64 `json:"destinationId"`
		}
		if !decode(r, &in) || in.DestinationID == 0 {
			badRequest(w, "destinationId is required")
			return
		}
		if _, err := a.db.Exec(`INSERT INTO program_destination(program_id,destination_id) VALUES(?,?) ON CONFLICT(program_id,destination_id) DO UPDATE SET enabled=1`, programID, in.DestinationID); err != nil {
			conflictOrServer(w, err)
			return
		}
		if err := a.syncProgramWithRetry(r.Context(), programID); err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 201, map[string]bool{"ok": true})
		return
	}
	if len(pathParts(r.URL.Path, fmt.Sprintf("/api/v1/programs/%d/destinations/", programID))) == 1 && r.Method == http.MethodDelete {
		parts := pathParts(r.URL.Path, fmt.Sprintf("/api/v1/programs/%d/destinations/", programID))
		did, _ := strconv.ParseInt(parts[0], 10, 64)
		a.db.Exec(`DELETE FROM program_destination WHERE program_id=? AND destination_id=?`, programID, did)
		if err := a.syncProgram(r.Context(), programID); err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	methodNotAllowed(w)
}

// populateDestinationStatuses reads MediaMTX forward states and attaches a
// user-facing status to each configured destination. A destination can be
// bound to more than one program, so forwarding from any bound program is
// considered online; an error is reported only when no bound forward is live.
func (a *app) populateDestinationStatuses(ctx context.Context, destinations []destination) {
	if len(destinations) == 0 {
		return
	}
	rows, err := a.db.Query(`SELECT pd.destination_id,p.slug FROM program_destination pd JOIN program p ON p.id=pd.program_id WHERE pd.enabled=1`)
	if err != nil {
		for i := range destinations {
			destinations[i].StreamStatus = "other"
			destinations[i].StreamStatusReason = "暂时无法读取绑定关系"
		}
		return
	}
	bindings := map[int64][]string{}
	for rows.Next() {
		var destinationID int64
		var slug string
		if rows.Scan(&destinationID, &slug) == nil {
			bindings[destinationID] = append(bindings[destinationID], slug)
		}
	}
	rows.Close()
	forwardCache := map[string][]mediaForwardDestination{}
	forwardErrors := map[string]error{}
	forwardLoaded := map[string]bool{}
	for i := range destinations {
		d := &destinations[i]
		d.StreamStatus = "other"
		if !d.Enabled {
			d.StreamStatusReason = "目标已停用"
			continue
		}
		if !validForwardURL(d.URL) {
			d.StreamStatus = "offline"
			d.StreamStatusReason = "目标地址无效：必须包含 RTMP 应用路径，例如 /live"
			continue
		}
		slugs := bindings[d.ID]
		if len(slugs) == 0 {
			d.StreamStatusReason = "未绑定节目"
			continue
		}
		base := strings.ToLower(forwardBaseURL(d.URL))
		foundError := ""
		foundIdle := false
		mediaAPIUnavailable := false
		for _, slug := range slugs {
			if state, relayErr, ok := a.relayStatus(slug, d.ID); ok {
				if state == "forwarding" {
					d.StreamStatus = "online"
					d.StreamStatusReason = "FFmpeg 正在推流"
					break
				}
				if relayErr != "" && foundError == "" {
					foundError = relayErr
				}
			}
			forwards := forwardCache[slug]
			if !forwardLoaded[slug] {
				probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				forwards, forwardErrors[slug] = a.mtx.forwardDestinations(probeCtx, slug)
				cancel()
				forwardCache[slug] = forwards
				forwardLoaded[slug] = true
			}
			if forwardErrors[slug] != nil {
				mediaAPIUnavailable = true
				continue
			}
			for _, forward := range forwards {
				if !forwardDestinationMatches(base, forward.Conf.Dest) {
					continue
				}
				switch strings.ToLower(forward.State) {
				case "forwarding":
					d.StreamStatus = "online"
					d.StreamStatusReason = "MediaMTX 正在推流"
				case "error":
					if foundError == "" {
						foundError = forward.LastError
					}
				case "idle":
					foundIdle = true
				}
			}
		}
		if d.StreamStatus == "online" {
			continue
		}
		if foundError != "" {
			d.StreamStatus = "offline"
			d.StreamStatusReason = foundError
		} else if foundIdle {
			d.StreamStatusReason = "目标已配置，等待节目源"
		} else if mediaAPIUnavailable {
			d.StreamStatus = "offline"
			d.StreamStatusReason = "MediaMTX 状态接口暂时不可用，正在重试"
		} else {
			d.StreamStatusReason = "尚未建立 MediaMTX 推流连接"
		}
	}
}

func (a *app) programSchedules(w http.ResponseWriter, r *http.Request, programID int64) {
	if r.Method == http.MethodGet {
		rows, err := a.db.Query(`SELECT id,program_id,weekday,start_time,end_time,enabled FROM recording_schedule WHERE program_id=? ORDER BY weekday,start_time`, programID)
		if err != nil {
			serverError(w, err)
			return
		}
		defer rows.Close()
		out := []schedule{}
		for rows.Next() {
			var s schedule
			var e int
			rows.Scan(&s.ID, &s.ProgramID, &s.Weekday, &s.StartTime, &s.EndTime, &e)
			s.Enabled = e != 0
			out = append(out, s)
		}
		writeJSON(w, 200, out)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var in scheduleInput
	if !decode(r, &in) || in.Weekday < 0 || in.Weekday > 6 || !validTime(in.StartTime) || !validTime(in.EndTime) {
		badRequest(w, "weekday 0-6 and HH:MM start/end are required")
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	res, err := a.db.Exec(`INSERT INTO recording_schedule(program_id,weekday,start_time,end_time,enabled) VALUES(?,?,?,?,?)`, programID, in.Weekday, in.StartTime, in.EndTime, boolInt(enabled))
	if err != nil {
		serverError(w, err)
		return
	}
	id, _ := res.LastInsertId()
	writeJSON(w, 201, map[string]any{"id": id, "programId": programID, "weekday": in.Weekday, "startTime": in.StartTime, "endTime": in.EndTime, "enabled": enabled})
}

func (a *app) scheduleSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(r.URL.Path, "/api/v1/schedules/")
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		badRequest(w, "invalid schedule id")
		return
	}
	if r.Method == http.MethodDelete {
		_, err = a.db.Exec(`DELETE FROM recording_schedule WHERE id=?`, id)
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.Method != http.MethodPatch {
		methodNotAllowed(w)
		return
	}
	var in scheduleInput
	if !decode(r, &in) {
		badRequest(w, "invalid JSON")
		return
	}
	var s schedule
	var e int
	if err := a.db.QueryRow(`SELECT id,program_id,weekday,start_time,end_time,enabled FROM recording_schedule WHERE id=?`, id).Scan(&s.ID, &s.ProgramID, &s.Weekday, &s.StartTime, &s.EndTime, &e); err != nil {
		notFoundOrServer(w, err)
		return
	}
	s.Enabled = e != 0
	if in.Weekday >= 0 && in.Weekday <= 6 {
		s.Weekday = in.Weekday
	}
	if in.StartTime != "" {
		s.StartTime = in.StartTime
	}
	if in.EndTime != "" {
		s.EndTime = in.EndTime
	}
	if in.Enabled != nil {
		s.Enabled = *in.Enabled
	}
	if !validTime(s.StartTime) || !validTime(s.EndTime) {
		badRequest(w, "invalid time")
		return
	}
	_, err = a.db.Exec(`UPDATE recording_schedule SET weekday=?,start_time=?,end_time=?,enabled=? WHERE id=?`, s.Weekday, s.StartTime, s.EndTime, boolInt(s.Enabled), id)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, 200, s)
}

func (a *app) status(w http.ResponseWriter, r *http.Request) {
	paths, err := a.mtx.paths(r.Context())
	if err != nil {
		serverError(w, err)
		return
	}
	type item struct {
		Program      program       `json:"program"`
		Path         *mediaPath    `json:"path"`
		SignalStatus string        `json:"signalStatus"`
		SignalReason string        `json:"signalReason,omitempty"`
		Destinations []destination `json:"destinations"`
	}
	rows, err := a.db.Query(`SELECT id FROM program ORDER BY id`)
	if err != nil {
		serverError(w, err)
		return
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	out := []item{}
	for _, id := range ids {
		p, e := a.getProgram(id)
		if e != nil {
			continue
		}
		var path *mediaPath
		for _, candidate := range paths {
			if candidate.Name == p.Slug {
				candidateCopy := candidate
				path = &candidateCopy
				break
			}
		}
		ds, _ := a.programDestinationList(id)
		signalStatus, signalReason := a.cachedSignal(p.ID, path)
		out = append(out, item{Program: p, Path: path, SignalStatus: signalStatus, SignalReason: signalReason, Destinations: ds})
	}
	summary, err := a.overviewSummary()
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"time": time.Now().In(a.location).Format(time.RFC3339), "items": out, "summary": summary})
}

// overviewSummary keeps the dashboard counters in the same status response as
// the program runtime state, avoiding additional list requests on each refresh.
type overviewSummary struct {
	RecordingFiles int64 `json:"recordingFiles"`
	StreamAccesses int64 `json:"streamAccesses"`
	SystemEvents   int64 `json:"systemEvents"`
}

func (a *app) overviewSummary() (overviewSummary, error) {
	var summary overviewSummary
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM recording_file`).Scan(&summary.RecordingFiles); err != nil {
		return summary, err
	}
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM stream_access`).Scan(&summary.StreamAccesses); err != nil {
		return summary, err
	}
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM system_event`).Scan(&summary.SystemEvents); err != nil {
		return summary, err
	}
	return summary, nil
}

func (a *app) cachedSignal(id int64, path *mediaPath) (string, string) {
	if path != nil && path.Ready && path.Available {
		return "online", "MediaMTX 已接收有效轨道"
	}
	a.signalMu.RLock()
	probe, ok := a.signalCache[id]
	a.signalMu.RUnlock()
	if ok {
		return probe.Status, probe.Reason
	}
	if path != nil {
		return "offline", "MediaMTX 未收到有效轨道"
	}
	return "other", "等待信号检测"
}

func (a *app) signalMonitor(ctx context.Context) {
	a.refreshSignalCache(ctx)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refreshSignalCache(ctx)
		}
	}
}

func (a *app) refreshSignalCache(parent context.Context) {
	rows, err := a.db.Query(`SELECT id,name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at FROM program ORDER BY id`)
	if err != nil {
		return
	}
	programs := []program{}
	for rows.Next() {
		var p program
		var enabled, record int
		if rows.Scan(&p.ID, &p.Name, &p.Slug, &p.Kind, &p.SourceURL, &enabled, &record, &p.CreatedAt, &p.UpdatedAt) == nil {
			p.Enabled = enabled != 0
			p.RecordEnabled = record != 0
			programs = append(programs, p)
		}
	}
	rows.Close()
	for _, p := range programs {
		probeCtx, cancel := context.WithTimeout(parent, 4*time.Second)
		status, reason := a.probeSignal(probeCtx, p, nil)
		cancel()
		a.signalMu.Lock()
		a.signalCache[p.ID] = signalProbe{Status: status, Reason: reason, Checked: time.Now()}
		a.signalMu.Unlock()
	}
}

func (a *app) probeSignal(parent context.Context, p program, path *mediaPath) (string, string) {
	if path != nil && path.Ready && path.Available {
		return "online", "MediaMTX 已接收有效轨道"
	}
	u, err := url.Parse(strings.TrimSpace(p.SourceURL))
	if err != nil || u.Scheme == "" {
		return "other", "信号地址格式无法识别"
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "http" || scheme == "https" {
		ctx, cancel := context.WithTimeout(parent, 4*time.Second)
		defer cancel()
		probeURL := a.mediaSourceURL(u.String())
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		if reqErr != nil {
			return "other", "信号地址无法访问"
		}
		resp, requestErr := http.DefaultClient.Do(req)
		if requestErr != nil {
			return "offline", "原始信号连接失败"
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "offline", fmt.Sprintf("原始信号返回 HTTP %d", resp.StatusCode)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
		if readErr != nil {
			return "offline", "原始信号读取失败"
		}
		if strings.Contains(string(body), "#EXTM3U") {
			return "online", "原始 HLS 播放列表可访问"
		}
		return "other", "地址可访问但不是有效 HLS 播放列表"
	}
	if path != nil {
		return "offline", "MediaMTX 未收到有效轨道"
	}
	return "other", "该信号类型需通过 MediaMTX 检测"
}

func (a *app) splitterStatus(w http.ResponseWriter, r *http.Request) {
	if a.splitterStatusURL == "" {
		writeJSON(w, 200, map[string]any{"enabled": false, "available": false, "groups": []any{}})
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, a.splitterStatusURL+"/api/v1/status", nil)
	if err != nil {
		serverError(w, err)
		return
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		writeJSON(w, 200, map[string]any{"enabled": true, "available": false, "error": err.Error(), "groups": []any{}})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		writeJSON(w, 200, map[string]any{"enabled": true, "available": false, "error": fmt.Sprintf("splitter HTTP %d", resp.StatusCode), "groups": []any{}})
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		serverError(w, err)
		return
	}
	payload["enabled"] = true
	payload["available"] = true
	writeJSON(w, 200, payload)
}

func (a *app) recordings(w http.ResponseWriter, r *http.Request) {
	rows, err := a.db.Query(`SELECT f.id,f.program_id,p.name,p.kind,f.path,f.size_bytes,f.duration_seconds,f.created_at FROM recording_file f JOIN program p ON p.id=f.program_id ORDER BY f.created_at DESC LIMIT 500`)
	if err != nil {
		serverError(w, err)
		return
	}
	defer rows.Close()
	type file struct {
		ID          int64    `json:"id"`
		ProgramID   int64    `json:"programId"`
		ProgramName string   `json:"programName"`
		Kind        string   `json:"kind"`
		Path        string   `json:"path"`
		Size        int64    `json:"size"`
		Duration    *float64 `json:"duration"`
		CreatedAt   string   `json:"createdAt"`
	}
	out := []file{}
	staleIDs := []int64{}
	for rows.Next() {
		var f file
		if err := rows.Scan(&f.ID, &f.ProgramID, &f.ProgramName, &f.Kind, &f.Path, &f.Size, &f.Duration, &f.CreatedAt); err != nil {
			serverError(w, err)
			return
		}
		fileAbs, pathErr := a.recordingFilePath(f.Path)
		if pathErr != nil {
			// Never expose a database path that is outside the recordings root.
			continue
		}
		info, statErr := os.Stat(fileAbs)
		if errors.Is(statErr, os.ErrNotExist) || (statErr == nil && info.IsDir()) {
			// Recordings can be removed from the mounted disk or NAS outside this
			// service. Remove only the stale index row; never remove another file.
			staleIDs = append(staleIDs, f.ID)
			continue
		}
		if statErr != nil {
			// A transient mount or permission failure must not delete valid data.
			continue
		}
		f.Size = info.Size()
		f.Path = a.relativeRecordingPath(f.Path)
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		serverError(w, err)
		return
	}
	// The local SQLite deployment deliberately has one connection. Close the
	// result set before deleting stale rows, otherwise a missing file would
	// wait for the active SELECT and make the list request time out.
	if err := rows.Close(); err != nil {
		serverError(w, err)
		return
	}
	for _, id := range staleIDs {
		_, _ = a.db.Exec(`DELETE FROM recording_file WHERE id=?`, id)
	}
	writeJSON(w, 200, out)
}

func (a *app) executions(w http.ResponseWriter, r *http.Request) {
	// Older releases could deadlock before inserting execution rows. Reconcile
	// persisted recording files before reading the execution history so a
	// successful recording remains visible after an upgrade or restart.
	a.reconcileRecordingExecutions()
	rows, err := a.db.Query(`SELECT e.id,e.schedule_id,e.program_id,p.name,s.weekday,s.start_time,s.end_time,e.started_at,e.ended_at,e.status,e.error FROM recording_execution e JOIN program p ON p.id=e.program_id JOIN recording_schedule s ON s.id=e.schedule_id ORDER BY e.started_at DESC LIMIT 500`)
	if err != nil {
		serverError(w, err)
		return
	}
	defer rows.Close()
	type execution struct {
		ID           int64   `json:"id"`
		ScheduleID   int64   `json:"scheduleId"`
		ProgramID    int64   `json:"programId"`
		ProgramName  string  `json:"programName"`
		ScheduleName string  `json:"scheduleName"`
		StartedAt    string  `json:"startedAt"`
		EndedAt      *string `json:"endedAt"`
		Status       string  `json:"status"`
		Error        *string `json:"error"`
	}
	out := []execution{}
	for rows.Next() {
		var item execution
		var weekday int
		var startTime, endTime string
		if err := rows.Scan(&item.ID, &item.ScheduleID, &item.ProgramID, &item.ProgramName, &weekday, &startTime, &endTime, &item.StartedAt, &item.EndedAt, &item.Status, &item.Error); err != nil {
			serverError(w, err)
			return
		}
		item.ScheduleName = scheduleName(weekday, startTime, endTime)
		out = append(out, item)
	}
	writeJSON(w, 200, out)
}

func scheduleName(weekday int, start, end string) string {
	weekdays := []string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}
	day := "未知星期"
	if weekday >= 0 && weekday < len(weekdays) {
		day = weekdays[weekday]
	}
	return fmt.Sprintf("%s %s - %s", day, start, end)
}

func (a *app) relativeRecordingPath(path string) string {
	root := a.recordingsRoot
	if root == "" {
		root = "/recordings"
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

// recordingFilePath validates an indexed file before it is read or listed.
// The database stores absolute paths, but the service must only serve files
// from the configured recordings mount.
func (a *app) recordingFilePath(path string) (string, error) {
	root := a.recordingsRoot
	if root == "" {
		root = "/recordings"
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	fileAbs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, fileAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("invalid recording path")
	}
	return fileAbs, nil
}

func recordingNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, map[string]string{"message": "录制文件不存在或已删除"})
}

func (a *app) recordingFileContent(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/recordings/"), "/")
	if len(parts) != 2 || parts[0] != "file" {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || id <= 0 {
		recordingNotFound(w)
		return
	}
	var path string
	if err := a.db.QueryRow(`SELECT path FROM recording_file WHERE id=?`, id).Scan(&path); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			recordingNotFound(w)
		} else {
			serverError(w, err)
		}
		return
	}
	fileAbs, err := a.recordingFilePath(path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"message": "录制文件路径无效"})
		return
	}
	rel, _ := filepath.Rel(a.recordingsRoot, fileAbs)
	if r.Method == http.MethodDelete {
		if err := os.Remove(fileAbs); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				_, _ = a.db.Exec(`DELETE FROM recording_file WHERE id=?`, id)
				http.NotFound(w, r)
				return
			}
			serverError(w, err)
			return
		}
		if _, err := a.db.Exec(`DELETE FROM recording_file WHERE id=?`, id); err != nil {
			serverError(w, err)
			return
		}
		a.event("info", "recording_deleted", filepath.ToSlash(rel))
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	info, err := os.Stat(fileAbs)
	if err != nil || info.IsDir() {
		if errors.Is(err, os.ErrNotExist) {
			_, _ = a.db.Exec(`DELETE FROM recording_file WHERE id=?`, id)
		}
		recordingNotFound(w)
		return
	}
	if r.URL.Query().Get("preview") == "1" {
		if time.Since(info.ModTime()) < 10*time.Second {
			writeJSON(w, http.StatusOK, map[string]any{"status": "error", "message": "录制文件仍在生成，结束后才可以预览"})
			return
		}
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		cached, ready, previewErr := a.previewPath(id, fileAbs, info.ModTime())
		if previewErr != nil {
			serverError(w, previewErr)
			return
		}
		if !ready {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusAccepted, map[string]any{"status": "processing", "message": "预览文件正在生成，请稍候"})
			return
		}
		cachedInfo, statErr := os.Stat(cached)
		if statErr != nil {
			serverError(w, statErr)
			return
		}
		fileAbs, info = cached, cachedInfo
	}
	f, err := os.Open(fileAbs)
	if err != nil {
		recordingNotFound(w)
		return
	}
	defer f.Close()
	switch strings.ToLower(filepath.Ext(fileAbs)) {
	case ".mp4":
		w.Header().Set("Content-Type", "video/mp4")
	case ".ts":
		w.Header().Set("Content-Type", "video/mp2t")
	}
	if r.URL.Query().Get("download") == "1" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(fileAbs)))
	} else {
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", filepath.Base(fileAbs)))
		// Browsers commonly request bytes=0- for media metadata. Cap preview
		// responses so a large recording is fetched progressively in chunks.
		if r.URL.Query().Get("preview") == "1" {
			const previewChunk int64 = 64 * 1024 * 1024
			rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
			if rangeHeader == "" {
				r.Header.Set("Range", fmt.Sprintf("bytes=0-%d", previewChunk-1))
			} else if strings.HasPrefix(rangeHeader, "bytes=") {
				parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
				if len(parts) == 2 {
					start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
					if err == nil && start >= 0 {
						end := strings.TrimSpace(parts[1])
						if end == "" {
							r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+previewChunk-1))
						} else if parsedEnd, parseErr := strconv.ParseInt(end, 10, 64); parseErr == nil && parsedEnd >= start && parsedEnd-start+1 > previewChunk {
							r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+previewChunk-1))
						}
					}
				}
			}
		}
	}
	http.ServeContent(w, r, filepath.Base(fileAbs), info.ModTime(), f)
}

func (a *app) previewPath(id int64, source string, sourceMod time.Time) (string, bool, error) {
	root := a.previewRoot
	if root == "" {
		root = filepath.Join(filepath.Dir(a.recordingsRoot), "previews")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", false, err
	}
	// v3 prevents earlier long preview cache files from being reused after
	// the bounded preview implementation is deployed.
	dest := filepath.Join(root, fmt.Sprintf("v4-%d.mp4", id))
	if info, err := os.Stat(dest); err == nil && info.Size() > 0 && !info.ModTime().Before(sourceMod) {
		return dest, true, nil
	}
	a.previewMu.Lock()
	job := a.previewJobs[id]
	if job == nil || !job.sourceMod.Equal(sourceMod) || job.status == "ready" {
		job = &previewJob{status: "processing", sourceMod: sourceMod}
		a.previewJobs[id] = job
		go a.generatePreview(id, source, sourceMod, dest)
	}
	status := job.status
	errText := job.errText
	a.previewMu.Unlock()
	if status == "error" {
		return "", false, errors.New(errText)
	}
	return dest, false, nil
}

func (a *app) generatePreview(id int64, source string, sourceMod time.Time, dest string) {
	if a.previewSem != nil {
		a.previewSem <- struct{}{}
		defer func() { <-a.previewSem }()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	// Keep the MP4 extension on the temporary target so FFmpeg can select
	// the MP4 muxer before the completed file is atomically renamed.
	tmp := strings.TrimSuffix(dest, filepath.Ext(dest)) + ".tmp" + filepath.Ext(dest)
	_ = os.Remove(tmp)
	// Preview is intentionally a short clip. Rewrapping the entire recording
	// doubled NFS I/O and cache usage, which could starve the control service.
	cmd := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", source, "-t", "300", "-map", "0", "-c", "copy", "-movflags", "+faststart", tmp)
	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(tmp)
		a.previewMu.Lock()
		if job := a.previewJobs[id]; job != nil && job.sourceMod.Equal(sourceMod) {
			job.status = "error"
			job.errText = fmt.Sprintf("生成预览文件失败: %s", strings.TrimSpace(string(output)))
			if len(output) == 0 {
				job.errText = fmt.Sprintf("生成预览文件失败: %v", err)
			}
		}
		a.previewMu.Unlock()
		return
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		a.previewMu.Lock()
		if job := a.previewJobs[id]; job != nil && job.sourceMod.Equal(sourceMod) {
			job.status = "error"
			job.errText = fmt.Sprintf("生成预览文件失败: %v", err)
		}
		a.previewMu.Unlock()
		return
	}
	a.previewMu.Lock()
	if job := a.previewJobs[id]; job != nil && job.sourceMod.Equal(sourceMod) {
		job.status = "ready"
		job.errText = ""
	}
	a.previewMu.Unlock()
}

func (a *app) recordingPreviewStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/v1/recordings/preview/"), 10, 64)
	if err != nil || id <= 0 {
		http.NotFound(w, r)
		return
	}
	var path string
	if err := a.db.QueryRow(`SELECT path FROM recording_file WHERE id=?`, id).Scan(&path); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			recordingNotFound(w)
		} else {
			serverError(w, err)
		}
		return
	}
	fileAbs, err := a.recordingFilePath(path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"message": "录制文件路径无效"})
		return
	}
	info, err := os.Stat(fileAbs)
	if err != nil || info.IsDir() {
		if errors.Is(err, os.ErrNotExist) {
			_, _ = a.db.Exec(`DELETE FROM recording_file WHERE id=?`, id)
		}
		recordingNotFound(w)
		return
	}
	if time.Since(info.ModTime()) < 10*time.Second {
		writeJSON(w, http.StatusOK, map[string]any{"status": "error", "message": "录制文件仍在生成，结束后才可以预览"})
		return
	}
	_, ready, err := a.previewPath(id, fileAbs, info.ModTime())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "error", "message": err.Error()})
		return
	}
	if ready {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "url": fmt.Sprintf("/api/v1/recordings/file/%d?preview=1", id)})
		return
	}
	a.previewMu.Lock()
	job := a.previewJobs[id]
	status, message := "processing", "预览文件正在生成，请稍候"
	if job != nil && job.status == "error" {
		status, message = "error", job.errText
	}
	a.previewMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "message": message, "retryAfter": 1})
}

func (a *app) events(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		q := r.URL.Query()
		if q.Get("all") == "1" {
			if len(q) != 1 {
				badRequest(w, "清空全部日志时不能同时指定其他条件")
				return
			}
			result, err := a.db.Exec(`DELETE FROM system_event`)
			if err != nil {
				serverError(w, err)
				return
			}
			deleted, err := result.RowsAffected()
			if err != nil {
				serverError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
			return
		}
		beforeRaw := strings.TrimSpace(q.Get("before"))
		keepRaw := strings.TrimSpace(q.Get("keep"))
		if (beforeRaw == "" && keepRaw == "") || (beforeRaw != "" && keepRaw != "") {
			badRequest(w, "请指定 before 或 keep，且两者不能同时使用")
			return
		}
		var (
			result sql.Result
			err    error
		)
		if beforeRaw != "" {
			before, parseErr := time.Parse(time.RFC3339, beforeRaw)
			if parseErr != nil {
				badRequest(w, "before 必须是 RFC3339 时间，例如 2026-08-01T00:00:00Z")
				return
			}
			result, err = a.db.Exec(`DELETE FROM system_event WHERE created_at < ?`, before.UTC().Format(time.RFC3339))
		} else {
			keep, parseErr := strconv.Atoi(keepRaw)
			if parseErr != nil || keep < 1 || keep > 10000000 {
				badRequest(w, "keep 必须是 1 到 10000000 之间的整数")
				return
			}
			// Keep the newest N rows by id and remove older rows in one statement.
			result, err = a.db.Exec(`DELETE FROM system_event WHERE id NOT IN (SELECT id FROM system_event ORDER BY id DESC LIMIT ?)`, keep)
		}
		if err != nil {
			serverError(w, err)
			return
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			serverError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if r.URL.Query().Get("format") == "json" {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
		if pageSize < 1 || pageSize > 100 {
			pageSize = 20
		}
		var total int
		if err := a.db.QueryRow(`SELECT COUNT(*) FROM system_event`).Scan(&total); err != nil {
			serverError(w, err)
			return
		}
		offset := (page - 1) * pageSize
		rows, err := a.db.Query(`SELECT id,level,kind,message,created_at FROM system_event ORDER BY id DESC LIMIT ? OFFSET ?`, pageSize, offset)
		if err != nil {
			serverError(w, err)
			return
		}
		defer rows.Close()
		type event struct {
			ID        int64  `json:"id"`
			Level     string `json:"level"`
			Kind      string `json:"kind"`
			Message   string `json:"message"`
			CreatedAt string `json:"createdAt"`
		}
		out := []event{}
		for rows.Next() {
			var item event
			if err := rows.Scan(&item.ID, &item.Level, &item.Kind, &item.Message, &item.CreatedAt); err != nil {
				serverError(w, err)
				return
			}
			out = append(out, item)
		}
		writeJSON(w, 200, map[string]any{"items": out, "total": total, "page": page, "pageSize": pageSize})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case t := <-ticker.C:
			paths, err := a.mtx.paths(r.Context())
			payload := map[string]any{"time": t.In(a.location).Format(time.RFC3339), "paths": paths}
			if err != nil {
				payload = map[string]any{"time": t.In(a.location).Format(time.RFC3339), "error": err.Error()}
			}
			b, _ := json.Marshal(payload)
			fmt.Fprintf(w, "data: %s\n\n", b)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}

func (a *app) getProgram(id int64) (program, error) {
	var p program
	var e, r int
	if err := a.db.QueryRow(`SELECT id,name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at FROM program WHERE id=?`, id).Scan(&p.ID, &p.Name, &p.Slug, &p.Kind, &p.SourceURL, &e, &r, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return p, err
	}
	p.Enabled = e != 0
	p.RecordEnabled = r != 0
	return p, nil
}
func (a *app) getDestination(id int64) (destination, error) {
	var d destination
	var c string
	var e int
	if err := a.db.QueryRow(`SELECT id,name,url,protocol,stream_key_cipher,enabled FROM destination WHERE id=?`, id).Scan(&d.ID, &d.Name, &d.URL, &d.Protocol, &c, &e); err != nil {
		return d, err
	}
	d.Enabled = e != 0
	d.KeyMasked = maskKey(c)
	return d, nil
}
func (a *app) programDestinationList(id int64) ([]destination, error) {
	rows, err := a.db.Query(`SELECT d.id,d.name,d.url,d.protocol,d.stream_key_cipher,d.enabled FROM destination d JOIN program_destination pd ON pd.destination_id=d.id WHERE pd.program_id=? AND pd.enabled=1 AND d.enabled=1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []destination{}
	for rows.Next() {
		var d destination
		var c string
		var e int
		if err := rows.Scan(&d.ID, &d.Name, &d.URL, &d.Protocol, &c, &e); err != nil {
			return nil, err
		}
		d.Enabled = e != 0
		d.KeyMasked = maskKey(c)
		out = append(out, d)
	}
	return out, nil
}
func (a *app) syncProgram(ctx context.Context, id int64) error {
	p, err := a.getProgram(id)
	if err != nil {
		return err
	}
	dests, err := a.programDestinationList(id)
	if err != nil {
		return err
	}
	forwards := []map[string]string{}
	for _, d := range dests {
		if !validForwardURL(d.URL) {
			a.event("warn", "destination_error", fmt.Sprintf("推流目标 %s 地址协议无效，已跳过: %s", d.Name, d.URL))
			continue
		}
		row := struct{ cipher string }{}
		if err := a.db.QueryRow(`SELECT stream_key_cipher FROM destination WHERE id=?`, d.ID).Scan(&row.cipher); err != nil {
			return err
		}
		key := ""
		if row.cipher != "" {
			key, err = a.crypt.open(row.cipher)
			if err != nil {
				return err
			}
		}
		dest, err := buildForwardDestination(d.URL, key)
		if err != nil {
			a.event("warn", "destination_error", fmt.Sprintf("推流目标 %s 配置无效，已跳过", d.Name))
			continue
		}
		if !requiresFFmpegRelay(d.URL) {
			forwards = append(forwards, map[string]string{"dest": dest})
		}
	}
	recording := a.recordingSettings()
	body := map[string]any{"sourceOnDemand": false, "record": false, "forward": forwards,
		"recordPath": "/recordings/%path/%Y-%m-%d/%H-%M-%S-%f", "recordFormat": recording.RecordFormat,
		"recordPartDuration": recording.RecordPartDuration, "recordSegmentDuration": recording.RecordSegmentDuration,
		"recordDeleteAfter": recording.RecordDeleteAfter, "recordMaxPartSize": recording.RecordMaxPartSize}
	if isPublisherSource(p.SourceURL) {
		// A splitter publishes RTMP into this path. MediaMTX must not try to pull
		// publisher:// as if it were a network URL.
		body["source"] = "publisher"
	} else {
		body["source"] = a.mediaSourceURL(normalizeUDPSource(p.SourceURL))
	}
	if !p.Enabled && !isPublisherSource(p.SourceURL) {
		body["sourceOnDemand"] = true
	}
	if err := a.mtx.upsertPath(ctx, p.Slug, body); err != nil {
		return err
	}
	a.reconcileFFmpegRelays(p, dests)
	return nil
}

func requiresFFmpegRelay(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.RawQuery != ""
}

func (a *app) relayStatus(slug string, destinationID int64) (string, string, bool) {
	var id int64
	if err := a.db.QueryRow(`SELECT id FROM program WHERE slug=?`, slug).Scan(&id); err != nil {
		return "", "", false
	}
	a.relayMu.Lock()
	r, ok := a.relays[relayKey{programID: id, destinationID: destinationID}]
	if !ok {
		a.relayMu.Unlock()
		return "", "", false
	}
	state, lastError := r.state, r.lastError
	a.relayMu.Unlock()
	return state, lastError, true
}

func (a *app) reconcileFFmpegRelays(p program, destinations []destination) {
	desired := map[relayKey]string{}
	for _, d := range destinations {
		if !requiresFFmpegRelay(d.URL) {
			continue
		}
		cipherText := ""
		if err := a.db.QueryRow(`SELECT stream_key_cipher FROM destination WHERE id=?`, d.ID).Scan(&cipherText); err != nil {
			continue
		}
		key := ""
		if cipherText != "" {
			key, _ = a.crypt.open(cipherText)
		}
		dest, err := buildForwardDestination(d.URL, key)
		if err == nil {
			desired[relayKey{programID: p.ID, destinationID: d.ID}] = dest
		}
	}
	a.relayMu.Lock()
	for key, r := range a.relays {
		if key.programID == p.ID {
			if _, keep := desired[key]; !keep {
				r.cancel()
				delete(a.relays, key)
			}
		}
	}
	for key, dest := range desired {
		if current, exists := a.relays[key]; exists {
			if current.dest == dest {
				continue
			}
			current.cancel()
			delete(a.relays, key)
		}
		relayCtx, cancel := context.WithCancel(context.Background())
		a.relays[key] = &relayRuntime{cancel: cancel, dest: dest, state: "starting", startedAt: time.Now()}
		go a.runFFmpegRelay(relayCtx, key, p.Slug, dest)
	}
	a.relayMu.Unlock()
}

func (a *app) runFFmpegRelay(ctx context.Context, key relayKey, slug, dest string) {
	source := strings.TrimRight(a.mediaHLSBase, "/") + "/" + url.PathEscape(slug) + "/index.m3u8"
	for {
		cmd := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning", "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5", "-i", source, "-c", "copy", "-f", "flv", dest)
		var stderr strings.Builder
		cmd.Stderr = &stderr
		a.setRelayState(key, "forwarding", "")
		err := cmd.Run()
		if ctx.Err() != nil {
			return
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		a.setRelayState(key, "error", message)
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (a *app) setRelayState(key relayKey, state, lastError string) {
	a.relayMu.Lock()
	if r := a.relays[key]; r != nil {
		r.state = state
		r.lastError = lastError
	}
	a.relayMu.Unlock()
}

func validForwardURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || u.Path == "" || u.Path == "/" {
		return false
	}
	return strings.EqualFold(u.Scheme, "rtmp") || strings.EqualFold(u.Scheme, "rtmps")
}

func forwardProtocolMatches(raw, protocol string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Scheme, defaultProtocol(protocol))
}

func buildForwardDestination(raw, streamKey string) (string, error) {
	if !validForwardURL(raw) || strings.ContainsAny(streamKey, "#\r\n\t ") {
		return "", errors.New("invalid RTMP destination")
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	u.Fragment = ""
	base := strings.TrimRight(u.String(), "/")
	if streamKey == "" {
		return base, nil
	}
	return base + "#" + streamKey, nil
}

func forwardBaseURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimRight(strings.TrimSpace(raw), "/#")
	}
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

func forwardDestinationMatches(configured, actual string) bool {
	want := strings.ToLower(strings.TrimSpace(configured))
	got := strings.ToLower(strings.TrimSpace(actual))
	return got == want || strings.HasPrefix(got, want+"#")
}

// validSourceURL allows only pull formats handled by MediaMTX plus the local
// publisher marker. Keep this separate from forwarding URLs: an input can use
// UDP, SRT or RTSP while a distribution destination is RTMP(S) only.
func validSourceURL(raw string) bool {
	v := strings.TrimSpace(raw)
	if v == "" || len(v) > 2048 || strings.ContainsAny(v, "\r\n\t ") {
		return false
	}
	if v == "publisher" || v == "publisher://" {
		return true
	}
	u, err := url.Parse(v)
	if err != nil || u.Host == "" {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "rtmp", "rtmps", "rtsp", "rtsps", "srt", "rist", "udp", "udp+mpegts":
		return true
	case "publisher":
		// publisher://<path> is an internal marker for a splitter-fed path,
		// not a network URL. Keep query/userinfo out of this marker because it
		// is never consumed by MediaMTX and would make the source ambiguous.
		return u.User == nil && u.RawQuery == "" && u.Fragment == ""
	default:
		return false
	}
}

func isPublisherSource(raw string) bool {
	v := strings.TrimSpace(strings.ToLower(raw))
	return v == "publisher" || strings.HasPrefix(v, "publisher://")
}

// Accept the familiar VLC/FFmpeg udp:// form for a single MPEG-TS program.
// MediaMTX uses the explicit udp+mpegts:// source scheme and does not use the
// FFmpeg-style '@' bind marker.
func normalizeUDPSource(raw string) string {
	v := strings.TrimSpace(raw)
	if len(v) < len("udp://") || !strings.EqualFold(v[:len("udp://")], "udp://") {
		return raw
	}
	rest := strings.TrimPrefix(v[len("udp://"):], "@")
	return "udp+mpegts://" + rest
}

func (a *app) mediaSourceURL(raw string) string {
	if a.mediaSourceProxyBase == "" {
		return raw
	}
	source, err := url.Parse(raw)
	proxy, proxyErr := url.Parse(a.mediaSourceProxyBase)
	if err != nil || proxyErr != nil || source.Host == "" || proxy.Host == "" || proxy.Scheme == "" || (source.Scheme != "http" && source.Scheme != "https") {
		return raw
	}
	proxy.Path = strings.TrimRight(proxy.Path, "/") + "/" + strings.TrimLeft(source.Path, "/")
	proxy.RawQuery = source.RawQuery
	return proxy.String()
}

func (a *app) syncAllPrograms(ctx context.Context) error {
	rows, err := a.db.Query(`SELECT id FROM program ORDER BY id`)
	if err != nil {
		return err
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, id := range ids {
		if err := a.syncProgram(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) recordingSettings() globalSettings {
	var s globalSettings
	_ = a.db.QueryRow(`SELECT record_format,record_bitrate_kbps,record_duration,record_part_duration,record_segment_duration,record_delete_after,record_max_part_size FROM global_setting WHERE id=1`).Scan(&s.RecordFormat, &s.RecordBitrateKbps, &s.RecordDuration, &s.RecordPartDuration, &s.RecordSegmentDuration, &s.RecordDeleteAfter, &s.RecordMaxPartSize)
	if s.RecordFormat == "" {
		s.RecordFormat = "fmp4"
	}
	if s.RecordPartDuration == "" {
		s.RecordPartDuration = "1s"
	}
	if s.RecordDuration == "" {
		s.RecordDuration = s.RecordSegmentDuration
	}
	if s.RecordDuration == "0s" {
		s.RecordSegmentDuration = "24h"
	} else if s.RecordDuration != "" {
		// The user-facing duration is the single source of truth. Keep the
		// legacy MediaMTX field aligned for existing databases as well.
		s.RecordSegmentDuration = s.RecordDuration
	}
	if s.RecordSegmentDuration == "" {
		s.RecordSegmentDuration = "30s"
	}
	if s.RecordDeleteAfter == "" {
		s.RecordDeleteAfter = "0s"
	}
	if s.RecordMaxPartSize == "" {
		s.RecordMaxPartSize = "50M"
	}
	return s
}

func (a *app) scheduler(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tickSchedule(ctx)
		}
	}
}

func (a *app) indexRecordings(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.scanRecordings()
		}
	}
}

func (a *app) scanRecordings() {
	root := a.recordingsRoot
	if root == "" {
		root = "/recordings"
	}
	programs, err := a.db.Query(`SELECT id,slug FROM program`)
	if err != nil {
		return
	}
	slugs := map[string]int64{}
	for programs.Next() {
		var id int64
		var slug string
		if programs.Scan(&id, &slug) == nil {
			slugs[slug] = id
		}
	}
	programs.Close()
	scheduleMatches := []recordingScheduleMatch{}
	if scheduleRows, scheduleErr := a.db.Query(`SELECT program_id,weekday,start_time,end_time FROM recording_schedule`); scheduleErr == nil {
		for scheduleRows.Next() {
			var s recordingScheduleMatch
			if scheduleRows.Scan(&s.programID, &s.weekday, &s.start, &s.end) == nil {
				scheduleMatches = append(scheduleMatches, s)
			}
		}
		scheduleRows.Close()
	}
	filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		ext := strings.ToLower(filepath.Ext(path))
		if walkErr != nil || entry.IsDir() || (ext != ".mp4" && ext != ".ts") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) < 2 {
			return nil
		}
		pid, ok := slugs[parts[0]]
		if !ok {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		createdAt := info.ModTime()
		if recordingStart, ok := parseRecordingStart(filepath.ToSlash(rel), a.location); ok {
			if plannedEnd, matched := scheduledRecordingEnd(pid, recordingStart, scheduleMatches, time.Now().In(a.location), a.location); matched {
				createdAt = plannedEnd
			}
		}
		_, _ = a.db.Exec(`INSERT INTO recording_file(program_id,path,size_bytes,created_at) VALUES(?,?,?,?) ON CONFLICT(path) DO UPDATE SET size_bytes=excluded.size_bytes,created_at=excluded.created_at`, pid, path, info.Size(), createdAt.UTC().Format(time.RFC3339))
		return nil
	})
	a.reconcileRecordingExecutions()
}

type recordingScheduleMatch struct {
	programID int64
	weekday   int
	start     string
	end       string
}

// parseRecordingStart extracts the start timestamp from the standard
// /program/YYYY-MM-DD/HH-MM-SS-ffffff.ext recording path.
func parseRecordingStart(rel string, loc *time.Location) (time.Time, bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 3 || len(parts[1]) != len("2006-01-02") {
		return time.Time{}, false
	}
	name := filepath.Base(parts[len(parts)-1])
	if len(name) < len("15-04-05") {
		return time.Time{}, false
	}
	value := parts[1] + "/" + name[:len("15-04-05")]
	t, err := time.ParseInLocation("2006-01-02/15-04-05", value, loc)
	return t, err == nil
}

func scheduledRecordingEnd(programID int64, recordingStart time.Time, schedules []recordingScheduleMatch, now time.Time, loc *time.Location) (time.Time, bool) {
	for _, s := range schedules {
		if s.programID != programID {
			continue
		}
		for dayOffset := -1; dayOffset <= 0; dayOffset++ {
			day := time.Date(recordingStart.Year(), recordingStart.Month(), recordingStart.Day()+dayOffset, 0, 0, 0, 0, loc)
			if int(day.Weekday()) != s.weekday {
				continue
			}
			from, to, ok := scheduleInterval(day, s.start, s.end, loc)
			if !ok || recordingStart.Before(from) || !recordingStart.Before(to) || to.After(now) {
				continue
			}
			return to, true
		}
	}
	return time.Time{}, false
}

// reconcileRecordingExecutions rebuilds missing execution rows from completed
// recording files. MediaMTX writes a file when a segment closes, so a file
// timestamp can be after the configured schedule end. A two-hour tolerance
// covers the production one-hour segment setting without treating unrelated
// recordings as scheduled executions.
func (a *app) reconcileRecordingExecutions() {
	// A process restart can interrupt the asynchronous file verification. Do
	// not leave an execution stuck in "verifying" forever.
	_, _ = a.db.Exec(`UPDATE recording_execution SET status=?,error=? WHERE status=? AND ended_at IS NOT NULL AND ended_at < ?`, "failed", "服务重启后仍未检测到有效录制文件", "verifying", time.Now().UTC().Add(-2*time.Minute).Format(time.RFC3339))
	type scheduleInfo struct {
		id        int64
		programID int64
		weekday   int
		start     string
		end       string
	}
	scheduleRows, err := a.db.Query(`SELECT id,program_id,weekday,start_time,end_time FROM recording_schedule`)
	if err != nil {
		return
	}
	schedules := []scheduleInfo{}
	for scheduleRows.Next() {
		var s scheduleInfo
		if scheduleRows.Scan(&s.id, &s.programID, &s.weekday, &s.start, &s.end) == nil {
			schedules = append(schedules, s)
		}
	}
	scheduleRows.Close()
	today := time.Now().In(a.location)
	for _, s := range schedules {
		for dayOffset := -1; dayOffset <= 0; dayOffset++ {
			day := time.Date(today.Year(), today.Month(), today.Day()+dayOffset, 0, 0, 0, 0, a.location)
			if int(day.Weekday()) != s.weekday {
				continue
			}
			start, end, ok := scheduleInterval(day, s.start, s.end, a.location)
			if !ok {
				continue
			}
			dayStart := start.UTC().Format(time.RFC3339)
			dayEnd := start.Add(24 * time.Hour).UTC().Format(time.RFC3339)
			var executionCount int
			if err := a.db.QueryRow(`SELECT COUNT(*) FROM recording_execution WHERE schedule_id=? AND started_at>=? AND started_at<?`, s.id, dayStart, dayEnd).Scan(&executionCount); err != nil || executionCount > 0 {
				continue
			}
			var fileCount int
			if err := a.db.QueryRow(`SELECT COUNT(*) FROM recording_file WHERE program_id=? AND size_bytes>0 AND created_at>=? AND created_at<=?`, s.programID, dayStart, end.Add(2*time.Hour).UTC().Format(time.RFC3339)).Scan(&fileCount); err != nil || fileCount == 0 {
				continue
			}
			status := "running"
			var ended any
			if !time.Now().In(a.location).Before(end) {
				status = "completed"
				ended = end.UTC().Format(time.RFC3339)
			}
			_, _ = a.db.Exec(`INSERT INTO recording_execution(schedule_id,program_id,started_at,ended_at,status) VALUES(?,?,?,?,?)`, s.id, s.programID, dayStart, ended, status)
		}
	}
}

func scheduleInterval(day time.Time, start, end string, loc *time.Location) (time.Time, time.Time, bool) {
	sh, sm, ok := parseHM(start)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	eh, em, ok := parseHM(end)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	from := time.Date(day.Year(), day.Month(), day.Day(), sh, sm, 0, 0, loc)
	toDay := day
	if eh*60+em <= sh*60+sm {
		toDay = day.AddDate(0, 0, 1)
	}
	to := time.Date(toDay.Year(), toDay.Month(), toDay.Day(), eh, em, 0, 0, loc)
	return from, to, true
}
func (a *app) tickSchedule(ctx context.Context) {
	now := time.Now().In(a.location)
	rows, err := a.db.Query(`SELECT s.id,s.program_id,s.weekday,s.start_time,s.end_time,s.enabled,p.record_enabled,p.slug FROM recording_schedule s JOIN program p ON p.id=s.program_id WHERE s.enabled=1 AND p.enabled=1 AND p.record_enabled=1`)
	if err != nil {
		return
	}
	type scheduleChoice struct {
		id   int64
		slug string
	}
	desired := map[int64]scheduleChoice{}
	for rows.Next() {
		var id, pid, w int
		var start, end, slug string
		var enabled, rec int
		if err := rows.Scan(&id, &pid, &w, &start, &end, &enabled, &rec, &slug); err != nil {
			continue
		}
		if enabled == 0 || rec == 0 {
			continue
		}
		if activeSlot(now, w, start, end) {
			// Keep the first matching schedule stable if overlapping schedules
			// exist for the same program.
			if _, exists := desired[int64(pid)]; !exists {
				desired[int64(pid)] = scheduleChoice{id: int64(id), slug: slug}
			}
		}
	}
	rows.Close()
	prows, err := a.db.Query(`SELECT id,slug FROM program WHERE enabled=1`)
	if err != nil {
		return
	}
	type programState struct {
		id   int64
		slug string
	}
	programs := []programState{}
	for prows.Next() {
		var p programState
		if prows.Scan(&p.id, &p.slug) == nil {
			programs = append(programs, p)
		}
	}
	prows.Close()
	for _, p := range programs {
		choice, scheduled := desired[p.id]
		want := scheduled
		a.stateMu.Lock()
		was := a.active[p.id]
		known := a.activeKnown[p.id]
		a.stateMu.Unlock()
		if want != was || !known {
			err := a.mtx.patchRecord(ctx, p.slug, want)
			if err != nil {
				a.event("error", "recording", fmt.Sprintf("%s: %v", p.slug, err))
				continue
			}
			a.stateMu.Lock()
			a.active[p.id] = want
			a.activeKnown[p.id] = true
			a.stateMu.Unlock()
			if want {
				a.startExecution(p.id, choice.id)
				a.event("info", "recording_started", p.slug)
			} else if known && was {
				a.stopExecution(p.id)
				a.event("info", "recording_stopped", p.slug)
			}
		}
	}
}

func (a *app) startExecution(programID, scheduleID int64) {
	if scheduleID <= 0 {
		return
	}
	_, _ = a.db.Exec(`INSERT INTO recording_execution(schedule_id,program_id,started_at,status) VALUES(?,?,?,?)`, scheduleID, programID, time.Now().UTC().Format(time.RFC3339), "running")
}

func (a *app) stopExecution(programID int64) {
	now := time.Now().UTC().Format(time.RFC3339)
	rows, err := a.db.Query(`SELECT id,started_at FROM recording_execution WHERE program_id=? AND ended_at IS NULL`, programID)
	if err != nil {
		return
	}
	type pendingExecution struct {
		id        int64
		startedAt string
	}
	pending := []pendingExecution{}
	for rows.Next() {
		var item pendingExecution
		if rows.Scan(&item.id, &item.startedAt) == nil {
			pending = append(pending, item)
		}
	}
	rows.Close()
	for _, item := range pending {
		// MediaMTX may need a few seconds to close the current segment. Verify
		// the resulting file before declaring the execution successful.
		_, _ = a.db.Exec(`UPDATE recording_execution SET ended_at=?,status=?,error=NULL WHERE id=?`, now, "verifying", item.id)
		go a.finalizeExecution(item.id, programID, item.startedAt, now)
	}
}

func (a *app) finalizeExecution(executionID, programID int64, startedAt, endedAt string) {
	deadline := time.Now().Add(90 * time.Second)
	for {
		a.scanRecordings()
		var files int
		if err := a.db.QueryRow(`SELECT COUNT(*) FROM recording_file WHERE program_id=? AND size_bytes>0 AND created_at>=? AND created_at<=?`, programID, startedAt, time.Now().UTC().Add(2*time.Minute).Format(time.RFC3339)).Scan(&files); err == nil && files > 0 {
			_, _ = a.db.Exec(`UPDATE recording_execution SET status=?,error=NULL WHERE id=?`, "completed", executionID)
			return
		}
		if time.Now().After(deadline) {
			_, _ = a.db.Exec(`UPDATE recording_execution SET status=?,error=? WHERE id=?`, "failed", "录制结束后未检测到有效录制文件", executionID)
			return
		}
		time.Sleep(5 * time.Second)
	}
}

func activeSlot(now time.Time, weekday int, start, end string) bool {
	sh, sm, ok := parseHM(start)
	if !ok {
		return false
	}
	eh, em, ok := parseHM(end)
	if !ok {
		return false
	}
	cur := now.Hour()*60 + now.Minute()
	s, e := sh*60+sm, eh*60+em
	day := int(now.Weekday())
	if s < e {
		return day == weekday && cur >= s && cur < e
	}
	if s > e {
		return (day == weekday && cur >= s) || (day == (weekday+1)%7 && cur < e)
	}
	return day == weekday
}
func parseHM(v string) (int, int, bool) {
	p := strings.Split(v, ":")
	if len(p) != 2 {
		return 0, 0, false
	}
	h, e1 := strconv.Atoi(p[0])
	m, e2 := strconv.Atoi(p[1])
	return h, m, e1 == nil && e2 == nil && h >= 0 && h < 24 && m >= 0 && m < 60
}

func (a *app) event(level, kind, message string) {
	a.db.Exec(`INSERT INTO system_event(level,kind,message,created_at) VALUES(?,?,?,?)`, level, kind, message, time.Now().UTC().Format(time.RFC3339))
}
func validSlug(v string) bool { return regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(v) }
func validTime(v string) bool { _, _, ok := parseHM(v); return ok }
func defaultKind(v string) string {
	if v == "audio" {
		return "audio"
	}
	return "video"
}
func defaultProtocol(v string) string {
	if v == "rtmps" {
		return "rtmps"
	}
	return "rtmp"
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func maskKey(cipherText string) string {
	if cipherText == "" {
		return ""
	}
	return "********"
}
func decode(r *http.Request, v any) bool {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v) == nil
}
func pathParts(path, prefix string) []string {
	v := strings.TrimPrefix(path, prefix)
	v = strings.Trim(v, "/")
	if v == "" {
		return nil
	}
	return strings.Split(v, "/")
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, 400, map[string]string{"error": msg})
}
func unauthorized(w http.ResponseWriter) {
	writeJSON(w, 401, map[string]string{"error": "unauthorized"})
}
func methodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, 405, map[string]string{"error": "method not allowed"})
}
func serverError(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	writeJSON(w, 500, map[string]string{"error": "internal server error"})
}
func notFoundOrServer(w http.ResponseWriter, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
	} else {
		serverError(w, err)
	}
}
func conflictOrServer(w http.ResponseWriter, err error) {
	if strings.Contains(strings.ToLower(err.Error()), "unique") {
		writeJSON(w, 409, map[string]string{"error": "already exists"})
	} else {
		serverError(w, err)
	}
}
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
