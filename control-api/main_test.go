package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRestoreSessionsKeepsValidSessionAcrossRestart(t *testing.T) {
	db, err := sql.Open("sqlite", "file:session-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	validUntil := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO auth_session(token,username,role,expires_at) VALUES('valid','admin','admin',?)`, validUntil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO auth_session(token,username,role,expires_at) VALUES('expired','admin','admin',?)`, time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	a := &app{db: db, sessions: map[string]time.Time{}, sessionUsers: map[string]string{}, sessionRoles: map[string]string{}}
	a.restoreSessions()
	if _, ok := a.sessions["valid"]; !ok || a.sessionUsers["valid"] != "admin" {
		t.Fatal("valid persisted session was not restored")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM auth_session WHERE token='expired'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired session was not removed: count=%d err=%v", count, err)
	}
}

func TestActiveSlotSupportsNormalAndCrossMidnight(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	if !activeSlot(time.Date(2026, 8, 24, 10, 30, 0, 0, loc), 1, "10:00", "11:00") {
		t.Fatal("expected Monday daytime slot to be active")
	}
	if activeSlot(time.Date(2026, 8, 24, 11, 0, 0, 0, loc), 1, "10:00", "11:00") {
		t.Fatal("expected end boundary to be exclusive")
	}
	if !activeSlot(time.Date(2026, 8, 24, 23, 30, 0, 0, loc), 1, "23:00", "01:00") {
		t.Fatal("expected Monday cross-midnight slot to be active")
	}
	if !activeSlot(time.Date(2026, 8, 25, 0, 30, 0, 0, loc), 1, "23:00", "01:00") {
		t.Fatal("expected Tuesday continuation of cross-midnight slot to be active")
	}
	if activeSlot(time.Date(2026, 8, 25, 1, 0, 0, 0, loc), 1, "23:00", "01:00") {
		t.Fatal("expected cross-midnight end boundary to be exclusive")
	}
}

func TestValidSourceURL(t *testing.T) {
	valid := []string{
		"https://example.test/live.m3u8",
		"rtmp://relay.example.test/live/channel",
		"rtmps://relay.example.test/live/channel",
		"rtsp://camera.example.test:554/live",
		"srt://receiver.example.test:9000?mode=caller",
		"rist://receiver.example.test:8193",
		"udp://@224.2.2.1:10003",
		"udp+mpegts://224.2.2.1:10003",
		"publisher",
		"publisher://",
		"publisher://audio_encoder_1",
	}
	for _, raw := range valid {
		if !validSourceURL(raw) {
			t.Fatalf("expected valid source URL: %q", raw)
		}
	}
	for _, raw := range []string{"", "ftp://example.test/live", "rtmp://", "udp://", "https://", "not a url", "publisher://?query=bad", "publisher://user:pass@relay"} {
		if validSourceURL(raw) {
			t.Fatalf("expected invalid source URL: %q", raw)
		}
	}
}

func TestBuildForwardDestination(t *testing.T) {
	got, err := buildForwardDestination("rtmp://ingest.example.test:1935/live/", "abc123")
	if err != nil || got != "rtmp://ingest.example.test:1935/live#abc123" {
		t.Fatalf("unexpected forward destination: %q, %v", got, err)
	}
	if _, err := buildForwardDestination("rtmp://ingest.example.test:1935", "abc123"); err == nil {
		t.Fatal("destination without an application path was accepted")
	}
	if _, err := buildForwardDestination("rtmp://ingest.example.test:1935/live", "bad#key"); err == nil {
		t.Fatal("stream key containing delimiter was accepted")
	}
	got, err = buildForwardDestination("rtmp://tui.example.test/live/t1?txSecret=abc&txTime=123", "")
	if err != nil || got != "rtmp://tui.example.test/live/t1?txSecret=abc&txTime=123" {
		t.Fatalf("complete provider URL was not preserved: %q, %v", got, err)
	}
}

func TestForwardProtocolMatches(t *testing.T) {
	if !forwardProtocolMatches("rtmps://ingest.example.test/live", "rtmps") {
		t.Fatal("expected RTMPS protocol to match")
	}
	if forwardProtocolMatches("rtmp://ingest.example.test/live", "rtmps") {
		t.Fatal("RTMP and RTMPS mismatch was accepted")
	}
}

func TestForwardBaseURLStripsFragmentAndTrailingSlash(t *testing.T) {
	if got := forwardBaseURL("rtmp://user:pass@ingest.example.test:1935/live/#old-key"); got != "rtmp://user:pass@ingest.example.test:1935/live" {
		t.Fatalf("unexpected base URL: %q", got)
	}
}

func TestStaleMediaPathDetection(t *testing.T) {
	if !isStaleMediaPath(mediaPath{}) {
		t.Fatal("an empty HLS path should be considered stale")
	}
	if isStaleMediaPath(mediaPath{Ready: true}) {
		t.Fatal("a ready path must not be considered stale")
	}
	if isStaleMediaPath(mediaPath{Tracks: []string{"H264"}}) {
		t.Fatal("a path with tracks must not be considered stale")
	}
	if isStaleMediaPath(mediaPath{InboundBytes: 1}) {
		t.Fatal("a path receiving bytes must not be considered stale")
	}
}

func TestMediaPathRecoveryThresholdAndCooldown(t *testing.T) {
	a := &app{}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	if a.shouldRecoverMediaPath("bttv-1", base) {
		t.Fatal("the first stale observation must only start the timer")
	}
	if a.shouldRecoverMediaPath("bttv-1", base.Add(mediaPathStaleAfter-time.Second)) {
		t.Fatal("stale path must remain below the recovery threshold")
	}
	if !a.shouldRecoverMediaPath("bttv-1", base.Add(mediaPathStaleAfter)) {
		t.Fatal("stale path should recover after the threshold")
	}
	if a.shouldRecoverMediaPath("bttv-1", base.Add(mediaPathStaleAfter+time.Minute)) {
		t.Fatal("recovery cooldown should suppress repeated restarts")
	}
	if !a.shouldRecoverMediaPath("bttv-1", base.Add(mediaPathStaleAfter+mediaPathRetryAfter)) {
		t.Fatal("recovery should be retried after the cooldown")
	}
}

func TestMediaPathRecoveryRestoresActiveRecording(t *testing.T) {
	var patches int
	var recordValue bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/v3/config/paths/patch/bttv-1" {
			t.Fatalf("unexpected MediaMTX request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]bool
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		patches++
		recordValue = body["record"]
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	a := &app{mtx: &mediaMTX{baseURL: server.URL, client: server.Client()}, active: map[int64]bool{1: true}}
	a.restoreRecordingState(context.Background(), 1, "bttv-1")
	if patches != 1 || !recordValue {
		t.Fatalf("expected active recording to be restored, patches=%d record=%v", patches, recordValue)
	}

	a.active[1] = false
	a.restoreRecordingState(context.Background(), 1, "bttv-1")
	if patches != 1 {
		t.Fatalf("inactive recording should not be patched, patches=%d", patches)
	}
}

func TestProbeSignalUsesConfiguredHLSProxy(t *testing.T) {
	var requestedPath string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-VERSION:3\n"))
	}))
	defer proxy.Close()

	a := &app{mediaSourceProxyBase: proxy.URL + "/relay"}
	status, reason := a.probeSignal(context.Background(), program{SourceURL: "http://origin.example/live.m3u8?channel=1"}, nil)
	if status != "online" || reason != "原始 HLS 播放列表可访问" {
		t.Fatalf("unexpected probe result: status=%q reason=%q", status, reason)
	}
	if requestedPath != "/relay/live.m3u8?channel=1" {
		t.Fatalf("expected proxy request path, got %q", requestedPath)
	}
}

func TestScheduleInterval(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, loc)
	start, end, ok := scheduleInterval(day, "19:33", "20:15", loc)
	if !ok || start.Hour() != 19 || start.Minute() != 33 || end.Hour() != 20 || end.Minute() != 15 || !end.Equal(start.Add(42*time.Minute)) {
		t.Fatalf("unexpected daytime interval: %v - %v", start, end)
	}
	start, end, ok = scheduleInterval(day, "23:30", "01:00", loc)
	if !ok || !end.After(start) || end.Day() != 25 || end.Hour() != 1 {
		t.Fatalf("unexpected cross-midnight interval: %v - %v", start, end)
	}
}

func TestOverviewSummaryCounts(t *testing.T) {
	db, err := sql.Open("sqlite", "file:overview-summary-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,created_at,updated_at) VALUES('节目','overview-program','video','https://example.test/live.m3u8','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO recording_file(program_id,path,created_at) VALUES(1,'/recordings/overview.mp4','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO stream_access(name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,created_at,updated_at) VALUES('通道','overview-access','pub','cipher','read','cipher','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO system_event(level,kind,message,created_at) VALUES('info','system','test','now')`); err != nil {
		t.Fatal(err)
	}
	summary, err := (&app{db: db}).overviewSummary()
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordingFiles != 1 || summary.StreamAccesses != 1 || summary.SystemEvents != 1 {
		t.Fatalf("unexpected overview summary: %#v", summary)
	}
}

func TestReconcileRecordingExecutionsFromFile(t *testing.T) {
	db, err := sql.Open("sqlite", "file:reconcile-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	loc := time.FixedZone("CST", 8*60*60)
	now := time.Now().In(loc)
	start := now.Add(-2 * time.Hour)
	end := now.Add(-time.Hour)
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at) VALUES('节目','reconcile-program','video','https://example.test/live.m3u8',1,1,'now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO recording_schedule(program_id,weekday,start_time,end_time,enabled) VALUES(1,?,?,?,1)`, int(start.Weekday()), start.Format("15:04"), end.Format("15:04")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO recording_file(program_id,path,size_bytes,created_at) VALUES(1,'/recordings/reconcile-program/test.mp4',100,?)`, end.Add(30*time.Second).UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	a := &app{db: db, location: loc}
	a.reconcileRecordingExecutions()
	var status string
	if err := db.QueryRow(`SELECT status FROM recording_execution`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("expected completed reconciled execution, got %q", status)
	}
}

func TestRecordingsSkipsAndCleansMissingFiles(t *testing.T) {
	db, err := sql.Open("sqlite", "file:recordings-list-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(root+"/program", 0o755); err != nil {
		t.Fatal(err)
	}
	validPath := root + "/program/valid.mp4"
	if err := os.WriteFile(validPath, []byte("valid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at) VALUES('节目','program','video','https://example.test/live.m3u8',1,1,'now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO recording_file(program_id,path,size_bytes,created_at) VALUES(1,?,5,'now'),(1,?,0,'now')`, validPath, root+"/program/missing.mp4"); err != nil {
		t.Fatal(err)
	}
	a := &app{db: db, recordingsRoot: root}
	rr := httptest.NewRecorder()
	a.recordings(rr, httptest.NewRequest(http.MethodGet, "/api/v1/recordings", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rr.Code, rr.Body.String())
	}
	var files []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &files); err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0]["path"] != "program/valid.mp4" {
		t.Fatalf("unexpected recording list: %#v", files)
	}
	var missing int
	if err := db.QueryRow(`SELECT COUNT(*) FROM recording_file WHERE path LIKE '%missing.mp4'`).Scan(&missing); err != nil || missing != 0 {
		t.Fatalf("missing index was not cleaned: count=%d err=%v", missing, err)
	}
}

func TestTickScheduleDoesNotBlockSingleSQLiteConnection(t *testing.T) {
	db, err := sql.Open("sqlite", "file:schedule-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at) VALUES('节目','program-1','video','https://example.test/live.m3u8',1,1,'now','now')`); err != nil {
		t.Fatal(err)
	}
	loc := time.FixedZone("CST", 8*60*60)
	now := time.Now().In(loc)
	if _, err := db.Exec(`INSERT INTO recording_schedule(program_id,weekday,start_time,end_time,enabled) VALUES(1,?,?,?,1)`, int(now.Weekday()), "00:00", "23:59"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/v3/config/paths/patch/program-1" {
			t.Fatalf("unexpected MediaMTX request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	a := &app{
		db:          db,
		mtx:         &mediaMTX{baseURL: server.URL, client: server.Client()},
		location:    loc,
		active:      map[int64]bool{},
		activeKnown: map[int64]bool{},
	}
	done := make(chan struct{})
	go func() {
		a.tickSchedule(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("schedule tick blocked while writing the execution record")
	}
	var scheduleID int64
	var status string
	if err := db.QueryRow(`SELECT schedule_id,status FROM recording_execution`).Scan(&scheduleID, &status); err != nil {
		t.Fatal(err)
	}
	if scheduleID != 1 || status != "running" {
		t.Fatalf("unexpected execution: schedule=%d status=%s", scheduleID, status)
	}
}

func TestTickScheduleDoesNotEmitStoppedForUnknownProgram(t *testing.T) {
	db, err := sql.Open("sqlite", "file:schedule-initial-state?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,record_enabled,created_at,updated_at) VALUES('节目','program-unknown','video','https://example.test/live.m3u8',1,1,'now','now')`); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/v3/config/paths/patch/program-unknown" {
			t.Fatalf("unexpected MediaMTX request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	a := &app{db: db, mtx: &mediaMTX{baseURL: server.URL, client: server.Client()}, location: time.FixedZone("CST", 8*60*60), active: map[int64]bool{}, activeKnown: map[int64]bool{}}
	a.tickSchedule(context.Background())
	var stopped int
	if err := db.QueryRow(`SELECT COUNT(*) FROM system_event WHERE kind='recording_stopped'`).Scan(&stopped); err != nil {
		t.Fatal(err)
	}
	if stopped != 0 {
		t.Fatalf("initial inactive program must not emit recording_stopped, got %d", stopped)
	}
}

func TestReconcileMediaMTXDoesNotBlockSingleSQLiteConnection(t *testing.T) {
	db, err := sql.Open("sqlite", "file:reconcile-mediamtx-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO program(name,slug,kind,source_url,enabled,created_at,updated_at) VALUES('节目','missing-path','video','https://example.test/live.m3u8',1,'now','now')`); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v3/paths/list":
			_, _ = w.Write([]byte(`{"items":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v3/config/paths/get/missing-path":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/v3/config/paths/add/missing-path":
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected MediaMTX request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	a := &app{
		db:       db,
		mtx:      &mediaMTX{baseURL: server.URL, client: server.Client()},
		location: time.FixedZone("CST", 8*60*60),
	}
	done := make(chan struct{})
	go func() {
		a.reconcileMediaMTX(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("MediaMTX reconciliation blocked while reusing the only SQLite connection")
	}
}

func TestRecordingRoleRouteAllowlist(t *testing.T) {
	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/auth/me"},
		{http.MethodGet, "/api/v1/recordings"},
		{http.MethodGet, "/api/v1/recordings/file/12"},
		{http.MethodDelete, "/api/v1/recordings/file/12"},
		{http.MethodGet, "/api/v1/executions"},
	}
	for _, tc := range allowed {
		if !recordingRouteAllowed(&http.Request{Method: tc.method, URL: mustURL(tc.path)}) {
			t.Fatalf("expected route to be allowed: %s %s", tc.method, tc.path)
		}
	}
	for _, tc := range []struct {
		method string
		path   string
	}{{http.MethodGet, "/api/v1/programs"}, {http.MethodGet, "/api/v1/settings"}, {http.MethodPost, "/api/v1/auth/password"}, {http.MethodGet, "/api/v1/events"}, {http.MethodDelete, "/api/v1/events?keep=10"}} {
		if recordingRouteAllowed(&http.Request{Method: tc.method, URL: mustURL(tc.path)}) {
			t.Fatalf("expected route to be denied: %s %s", tc.method, tc.path)
		}
	}
}

func TestEventsCleanupRequiresFilter(t *testing.T) {
	db, err := sql.Open("sqlite", "file:events-cleanup-filter?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	a := &app{db: db}
	r := httptest.NewRequest(http.MethodDelete, "/api/v1/events", nil)
	w := httptest.NewRecorder()
	a.events(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without cleanup filter, got %d", w.Code)
	}
}

func TestEventsCleanupBeforeAndKeep(t *testing.T) {
	db, err := sql.Open("sqlite", "file:events-cleanup-values?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	for _, created := range []string{"2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", "2026-08-03T00:00:00Z", "2026-08-04T00:00:00Z"} {
		if _, err := db.Exec(`INSERT INTO system_event(level,kind,message,created_at) VALUES('info','test','event',?)`, created); err != nil {
			t.Fatal(err)
		}
	}
	a := &app{db: db}
	r := httptest.NewRequest(http.MethodDelete, "/api/v1/events?before=2026-08-03T00:00:00Z", nil)
	w := httptest.NewRecorder()
	a.events(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"deleted":2`) {
		t.Fatalf("unexpected before cleanup response: %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodDelete, "/api/v1/events?keep=1", nil)
	w = httptest.NewRecorder()
	a.events(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"deleted":1`) {
		t.Fatalf("unexpected keep cleanup response: %d %s", w.Code, w.Body.String())
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM system_event`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("expected one event remaining, got %d", remaining)
	}
}

func TestEventsCleanupAllRequiresExplicitFlag(t *testing.T) {
	db, err := sql.Open("sqlite", "file:events-cleanup-all?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO system_event(level,kind,message,created_at) VALUES('info','test','event','2026-08-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	a := &app{db: db}
	r := httptest.NewRequest(http.MethodDelete, "/api/v1/events?all=1", nil)
	w := httptest.NewRecorder()
	a.events(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"deleted":1`) {
		t.Fatalf("unexpected all cleanup response: %d %s", w.Code, w.Body.String())
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM system_event`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expected all events removed, got %d", remaining)
	}
}

func TestMediaMTXDeletePathTreatsMissingAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"path not found"}`))
	}))
	defer server.Close()
	m := &mediaMTX{baseURL: server.URL, client: server.Client()}
	if err := m.deletePath(context.Background(), "missing"); err != nil {
		t.Fatalf("missing path should be idempotent, got %v", err)
	}
}

func TestNormalizeRoleDefaultsToAdmin(t *testing.T) {
	if normalizeRole("") != "admin" || normalizeRole("unknown") != "admin" {
		t.Fatal("empty and unknown roles must remain administrators")
	}
	if normalizeRole("recording") != "recording" || normalizeRole("RECORDING") != "recording" {
		t.Fatal("recording role was not normalized")
	}
}

func TestParseRecordingStart(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	got, ok := parseRecordingStart("bttv-1/2026-08-26/22-25-00-988129.mp4", loc)
	if !ok || got.Format("2006-01-02 15:04:05") != "2026-08-26 22:25:00" {
		t.Fatalf("unexpected recording start: %v, %v", got, ok)
	}
	if _, ok := parseRecordingStart("bttv-1/not-a-date/file.mp4", loc); ok {
		t.Fatal("invalid recording path was accepted")
	}
}

func TestScheduledRecordingEndUsesPlanEnd(t *testing.T) {
	loc := time.FixedZone("CST", 8*60*60)
	start := time.Date(2026, 8, 26, 22, 25, 0, 0, loc)
	schedules := []recordingScheduleMatch{{programID: 1, weekday: int(start.Weekday()), start: "22:25", end: "22:35"}}
	end, ok := scheduledRecordingEnd(1, start, schedules, time.Date(2026, 8, 26, 22, 36, 0, 0, loc), loc)
	if !ok || end.Format("2006-01-02 15:04:05") != "2026-08-26 22:35:00" {
		t.Fatalf("unexpected scheduled end: %v, %v", end, ok)
	}
	if _, ok := scheduledRecordingEnd(1, start, schedules, time.Date(2026, 8, 26, 22, 30, 0, 0, loc), loc); ok {
		t.Fatal("active recording was incorrectly assigned a future plan end")
	}
	crossStart := time.Date(2026, 8, 26, 23, 55, 0, 0, loc)
	crossSchedules := []recordingScheduleMatch{{programID: 1, weekday: int(crossStart.Weekday()), start: "23:00", end: "01:00"}}
	crossEnd, ok := scheduledRecordingEnd(1, crossStart, crossSchedules, time.Date(2026, 8, 27, 1, 1, 0, 0, loc), loc)
	if !ok || crossEnd.Format("2006-01-02 15:04:05") != "2026-08-27 01:00:00" {
		t.Fatalf("unexpected cross-midnight end: %v, %v", crossEnd, ok)
	}
}

func mustURL(path string) *url.URL {
	u, err := url.Parse(path)
	if err != nil {
		panic(err)
	}
	return u
}

func TestSecretBoxRoundTrip(t *testing.T) {
	s := newSecretBox("test-key")
	cipherText, err := s.seal("stream-key")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.open(cipherText)
	if err != nil || plain != "stream-key" {
		t.Fatalf("round trip failed: %q, %v", plain, err)
	}
}

func TestValidateExpiry(t *testing.T) {
	if got, err := validateExpiry(""); err != nil || got != "" {
		t.Fatalf("empty expiry should be allowed: %q, %v", got, err)
	}
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if got, err := validateExpiry(future); err != nil || got == "" {
		t.Fatalf("future expiry rejected: %q, %v", got, err)
	}
	if _, err := validateExpiry("not-a-timestamp"); err == nil {
		t.Fatal("invalid expiry was accepted")
	}
	if _, err := validateExpiry(time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)); err == nil {
		t.Fatal("past expiry was accepted")
	}
}

func TestMediaAuthForGeneratedStream(t *testing.T) {
	db, err := sql.Open("sqlite", "file:media-auth-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	crypt := newSecretBox("media-auth-test-key")
	pubCipher, _ := crypt.seal("publish-secret")
	readCipher, _ := crypt.seal("read-secret")
	_, err = db.Exec(`INSERT INTO stream_access(name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,created_at,updated_at) VALUES(?,?,?,?,?, ?,1,?,?)`, "Test", "access-test", "pub-user", pubCipher, "read-user", readCipher, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	a := &app{db: db, crypt: crypt}
	call := func(action, user, password string) int {
		body, _ := json.Marshal(map[string]string{"action": action, "path": "access-test", "user": user, "password": password})
		r := httptest.NewRequest(http.MethodPost, "/api/v1/media-auth", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		a.mediaAuth(w, r)
		return w.Code
	}
	if got := call("publish", "pub-user", "publish-secret"); got != http.StatusOK {
		t.Fatalf("valid publish credentials returned %d", got)
	}
	if got := call("publish", "pub-user", "wrong"); got != http.StatusUnauthorized {
		t.Fatalf("invalid publish credentials returned %d", got)
	}
	if got := call("read", "read-user", "read-secret"); got != http.StatusOK {
		t.Fatalf("valid read credentials returned %d", got)
	}
	if got := call("read", "pub-user", "publish-secret"); got != http.StatusUnauthorized {
		t.Fatalf("publish credentials were accepted for read: %d", got)
	}
}

func TestPublicHLSProxyUsesStoredReadCredential(t *testing.T) {
	db, err := sql.Open("sqlite", "file:public-hls-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	crypt := newSecretBox("public-hls-test-key")
	pubCipher, _ := crypt.seal("publish-secret")
	readCipher, _ := crypt.seal("read-secret")
	_, err = db.Exec(`INSERT INTO stream_access(name,slug,publish_user,publish_pass_cipher,read_user,read_pass_cipher,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?)`, "Test", "public-hls", "pub-user", pubCipher, "read-user", readCipher, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "read-user" || pass != "read-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if got := r.URL.Query().Get("session"); got != "media-session" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Query().Has("user") || r.URL.Query().Has("pass") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXTINF:4,\nsegment.m4s\n"))
	}))
	defer media.Close()
	a := &app{db: db, crypt: crypt, mediaHLSBase: media.URL, mtx: &mediaMTX{client: media.Client()}}
	w := httptest.NewRecorder()
	a.hlsProxy(w, httptest.NewRequest(http.MethodGet, "/hls/public-hls/index.m3u8?session=media-session&user=legacy-user&pass=legacy-pass", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected HLS status: %d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "user=") || strings.Contains(w.Body.String(), "pass=") {
		t.Fatalf("public playlist leaked credentials: %s", w.Body.String())
	}
}

func TestStreamAccessURLsIncludePublicHLSPlaylist(t *testing.T) {
	a := &app{}
	d := streamAccessDB{Slug: "partner_1", PublishUser: "pub", ReadUser: "read"}
	old := os.Getenv("PUBLIC_HLS_BASE_URL")
	defer os.Setenv("PUBLIC_HLS_BASE_URL", old)
	_ = os.Setenv("PUBLIC_HLS_BASE_URL", "https://stream.example/hls")
	_, _, hls := a.streamAccessURLs(d, "pub-pass", "read-pass")
	if hls != "https://stream.example/hls/partner_1/index.m3u8" {
		t.Fatalf("unexpected HLS URL: %s", hls)
	}
}

func TestPublisherSource(t *testing.T) {
	if !isPublisherSource("publisher://audio_encoder_1") || !isPublisherSource("publisher") {
		t.Fatal("publisher source was not recognized")
	}
	if isPublisherSource("https://example.test/live.m3u8") {
		t.Fatal("HLS source was incorrectly recognized as publisher")
	}
}

func TestNormalizeUDPSource(t *testing.T) {
	if got := normalizeUDPSource("udp://@224.2.2.10:12000"); got != "udp+mpegts://224.2.2.10:12000" {
		t.Fatalf("unexpected multicast source: %s", got)
	}
	if got := normalizeUDPSource("udp://192.168.10.20:12000?interface=eth0"); got != "udp+mpegts://192.168.10.20:12000?interface=eth0" {
		t.Fatalf("unexpected unicast source: %s", got)
	}
	if got := normalizeUDPSource("https://example.test/live.m3u8"); got != "https://example.test/live.m3u8" {
		t.Fatalf("non-UDP source was changed: %s", got)
	}
}

func TestRequiresFFmpegRelay(t *testing.T) {
	if !requiresFFmpegRelay("rtmp://tui.example/live/t1?txSecret=abc&txTime=123") {
		t.Fatal("signed RTMP destination should use the FFmpeg relay")
	}
	if requiresFFmpegRelay("rtmp://tui.example/live/t1") {
		t.Fatal("plain RTMP destination should remain a MediaMTX forward")
	}
}
