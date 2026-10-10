package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// AI credentials are encrypted separately, never included in public job JSON.
type newsRecovery struct {
	Size     int64            `json:"size"`
	Mtime    int64            `json:"mtime"`
	Duration float64          `json:"duration"`
	Smart    *newsSmartConfig `json:"smart,omitempty"`
}

func (a *app) recoveryCipher() (cipher.AEAD, error) {
	path := filepath.Join(filepath.Dir(a.newsRoot()), "news-recovery.key")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		data = make([]byte, 32)
		if _, err = rand.Read(data); err != nil {
			return nil, err
		}
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return nil, e
		}
		_, err = f.Write(data)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if err != nil {
		return nil, err
	}
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || (runtime.GOOS != "windows" && st.Mode().Perm() != 0600) || len(data) != 32 {
		return nil, errors.New("unsafe recovery key")
	}
	block, err := aes.NewCipher(data)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (a *app) saveNewsRecovery(dir string, job newsJob, info os.FileInfo, duration float64, smart *newsSmartConfig) error {
	c, err := a.recoveryCipher()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(newsRecovery{info.Size(), info.ModTime().UnixNano(), duration, smart})
	if err != nil {
		return err
	}
	nonce := make([]byte, c.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	aad := []byte(strconv.FormatInt(job.Owner, 10) + ":" + job.Key)
	sealed := c.Seal(nonce, nonce, raw, aad)
	if err = os.WriteFile(filepath.Join(dir, "recovery.tmp"), sealed, 0600); err != nil {
		return err
	}
	return os.Rename(filepath.Join(dir, "recovery.tmp"), filepath.Join(dir, "recovery.enc"))
}
func (a *app) loadNewsRecovery(dir string, job newsJob) (newsRecovery, error) {
	var out newsRecovery
	c, err := a.recoveryCipher()
	if err != nil {
		return out, err
	}
	path := filepath.Join(dir, "recovery.enc")
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() > 65536 {
		return out, errors.New("invalid recovery record")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) < c.NonceSize() {
		return out, errors.New("missing recovery record")
	}
	raw, err := c.Open(nil, data[:c.NonceSize()], data[c.NonceSize():], []byte(strconv.FormatInt(job.Owner, 10)+":"+job.Key))
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

// Called before serving requests, so list/status cannot race startup registration.
func (a *app) recoverNewsJobs() {
	owners, _ := os.ReadDir(a.newsRoot())
	for _, owner := range owners {
		id, err := strconv.ParseInt(owner.Name(), 10, 64)
		if err != nil || id < 1 || !owner.IsDir() || owner.Type()&os.ModeSymlink != 0 {
			continue
		}
		entries, _ := os.ReadDir(filepath.Join(a.newsRoot(), owner.Name()))
		for _, entry := range entries {
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !newsKey.MatchString(entry.Name()) {
				continue
			}
			dir := filepath.Join(a.newsRoot(), owner.Name(), entry.Name())
			job, err := readNewsJob(dir)
			if err != nil || job.Owner != id || job.Key != entry.Name() || job.ExpiresAt <= time.Now().Unix() || (job.State != "queued" && job.State != "running") {
				continue
			}
			fail := func() {
				job.State = "failed"
				job.Message = "任务无法安全恢复，请重新提交"
				_ = writeNewsJob(dir, job)
				a.event("error", "news_recovery_failed", "控制服务重启后任务无法安全恢复，请在我的任务中重新提交")
			}
			rec, err := a.loadNewsRecovery(dir, job)
			if err != nil {
				fail()
				continue
			}
			source, kind, info, err := a.newsSource(job.RecordingID)
			if err != nil || info.Size() != rec.Size || info.ModTime().UnixNano() != rec.Mtime || rec.Duration <= 0 || rec.Duration > 10800 {
				fail()
				continue
			}
			if job.Kind == "smart" {
				if rec.Smart == nil || rec.Smart.validate() != nil {
					fail()
					continue
				}
			} else if (job.Kind != "manual" && job.Kind != "preview") || validateNewsParts(job.Parts, rec.Duration) != nil {
				fail()
				continue
			}
			newsWork.Lock()
			if newsWork.active[dir] || len(newsWork.active) >= 8 {
				newsWork.Unlock()
				fail()
				continue
			}
			newsWork.active[dir] = true
			newsWork.Unlock()
			job.ControlRecoveries++
			job.State = "queued"
			job.Stage = "控制服务重启后正在恢复"
			job.Files = []string{}
			_ = writeNewsJob(dir, job)
			a.event("info", "news_recovered", "控制服务启动后已重新接管新闻处理任务")
			go a.runNewsJob(dir, job, rec.Smart, source, kind, info, rec.Duration)
		}
	}
}
