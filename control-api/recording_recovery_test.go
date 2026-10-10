package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEncryptedNewsRecoveryBinding(t *testing.T) {
	root := t.TempDir()
	a := &app{previewRoot: filepath.Join(root, "previews")}
	dir := filepath.Join(root, "job")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	os.WriteFile(source, []byte("immutable"), 0600)
	info, _ := os.Stat(source)
	job := newsJob{Owner: 1, Key: strings.Repeat("a", 32)}
	smart := &newsSmartConfig{ASRKey: "private-asr-test-key", ChatKey: "private-chat-test-key"}
	if err := a.saveNewsRecovery(dir, job, info, 60, smart); err != nil {
		t.Fatal(err)
	}
	wire, _ := os.ReadFile(filepath.Join(dir, "recovery.enc"))
	if strings.Contains(string(wire), smart.ASRKey) {
		t.Fatal("plaintext credential stored")
	}
	rec, err := a.loadNewsRecovery(dir, job)
	if err != nil || rec.Size != info.Size() || rec.Mtime != info.ModTime().UnixNano() || rec.Smart.ASRKey != smart.ASRKey {
		t.Fatal("recovery mismatch", err)
	}
	job.Owner = 2
	if _, err = a.loadNewsRecovery(dir, job); err == nil {
		t.Fatal("other owner decrypted record")
	}
	job.Owner = 1
	wire[len(wire)-1] ^= 1
	os.WriteFile(filepath.Join(dir, "recovery.enc"), wire, 0600)
	if _, err = a.loadNewsRecovery(dir, job); err == nil {
		t.Fatal("tampered record accepted")
	}
}
func TestStartupIgnoresExpiredAndCompleted(t *testing.T) {
	root := t.TempDir()
	a := &app{previewRoot: filepath.Join(root, "previews")}
	for i, state := range []string{"completed", "cancelled", "failed", "running"} {
		key := strings.Repeat(string(rune('a'+i)), 32)
		dir, _ := a.newsDir(1, key)
		os.MkdirAll(dir, 0700)
		job := newsJob{Key: key, Owner: 1, State: state, ExpiresAt: time.Now().Add(-time.Hour).Unix()}
		writeNewsJob(dir, job)
	}
	a.recoverNewsJobs()
	entries, _ := os.ReadDir(filepath.Join(a.newsRoot(), "1"))
	for _, entry := range entries {
		j, _ := readNewsJob(filepath.Join(a.newsRoot(), "1", entry.Name()))
		if j.ControlRecoveries != 0 {
			t.Fatal("expired task restarted")
		}
	}
}
