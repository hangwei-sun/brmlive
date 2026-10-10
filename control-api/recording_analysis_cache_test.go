package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAnalysisCacheRevisionAndContent(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	_ = os.WriteFile(source, []byte("fixture"), 0600)
	t.Setenv("NEWS_ANALYSIS_CACHE_ROOT", filepath.Join(root, "cache"))
	t.Setenv("NEWS_ANALYSIS_CACHE_REVISION", "model-prompt-v1")
	calls := 0
	run := func() ([]newsPart, error) { calls++; return []newsPart{{Title: "test", Start: 0.04, End: 2}}, nil }
	analyse := func() {
		_, err := cachedNewsAnalysis(context.Background(), source, 3, newsSmartConfig{}, func(int) {}, func(string, float64) {}, run)
		if err != nil {
			t.Fatal(err)
		}
	}
	analyse()
	analyse()
	if calls != 1 {
		t.Fatal("same source not reused")
	}
	t.Setenv("NEWS_ANALYSIS_CACHE_REVISION", "model-prompt-v2")
	analyse()
	if calls != 2 {
		t.Fatal("revision not invalidated")
	}
	_ = os.WriteFile(source, []byte("changed"), 0600)
	analyse()
	if calls != 3 {
		t.Fatal("content not invalidated")
	}
	if validAnalysisParts([]newsPart{{Title: "test", Start: 1, End: 2}, {Title: "test", Start: 1.9, End: 3}}, 3) {
		t.Fatal("overlap accepted")
	}
}
