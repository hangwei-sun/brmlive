package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type mediaMTX struct {
	baseURL string
	client  *http.Client
}

type mediaMTXHTTPError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *mediaMTXHTTPError) Error() string {
	return fmt.Sprintf("MediaMTX %s %s: HTTP %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

type mediaPath struct {
	Name         string   `json:"name"`
	Ready        bool     `json:"ready"`
	Available    bool     `json:"available"`
	Online       bool     `json:"online"`
	Tracks       []string `json:"tracks"`
	Source       any      `json:"source"`
	Readers      []any    `json:"readers"`
	InboundBytes uint64   `json:"inboundBytes"`
}

type mediaForwardDestination struct {
	Conf struct {
		Dest string `json:"dest"`
	} `json:"conf"`
	State         string `json:"state"`
	LastError     string `json:"lastError"`
	Protocol      string `json:"protocol"`
	OutboundBytes uint64 `json:"outboundBytes"`
}

func (m *mediaMTX) request(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, m.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &mediaMTXHTTPError{Method: method, Path: path, StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (m *mediaMTX) paths(ctx context.Context) ([]mediaPath, error) {
	var out struct {
		Items []mediaPath `json:"items"`
	}
	if err := m.request(ctx, http.MethodGet, "/v3/paths/list", nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

func (m *mediaMTX) forwardDestinations(ctx context.Context, pathName string) ([]mediaForwardDestination, error) {
	var out struct {
		Items []mediaForwardDestination `json:"items"`
	}
	pathValue := url.QueryEscape(pathName)
	if err := m.request(ctx, http.MethodGet, "/v3/paths/forward/list?path="+pathValue+"&itemsPerPage=100", nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}
func (m *mediaMTX) pathConfig(ctx context.Context, name string) (map[string]any, error) {
	var out map[string]any
	err := m.request(ctx, http.MethodGet, "/v3/config/paths/get/"+name, nil, &out)
	return out, err
}
func (m *mediaMTX) upsertPath(ctx context.Context, name string, body map[string]any) error {
	if _, err := m.pathConfig(ctx, name); err == nil {
		return m.request(ctx, http.MethodPatch, "/v3/config/paths/patch/"+name, body, nil)
	}
	return m.request(ctx, http.MethodPost, "/v3/config/paths/add/"+name, body, nil)
}
func (m *mediaMTX) patchRecord(ctx context.Context, name string, enabled bool) error {
	return m.request(ctx, http.MethodPatch, "/v3/config/paths/patch/"+name, map[string]any{"record": enabled}, nil)
}
func (m *mediaMTX) deletePath(ctx context.Context, name string) error {
	path := "/v3/config/paths/delete/" + name
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = m.request(ctx, http.MethodDelete, path, nil, nil)
		// DELETE is intentionally idempotent: a missing dynamic path already
		// has the desired state and must not produce a recurring warning.
		var httpErr *mediaMTXHTTPError
		if err == nil || (errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound) {
			return nil
		}
		if !isTransientMediaError(err) || attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func isTransientMediaError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection reset") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "eof")
}
