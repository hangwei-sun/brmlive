package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Private encoder health is authenticated. Only bounded operational metrics are read.
func (a *app) mediaAlertMonitor() {
	active := map[string]bool{}
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		base, token, err := gpuExportConfig()
		if err == nil && base != "" {
			current := map[string]string{}
			req, _ := http.NewRequest(http.MethodGet, base+"/health", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			resp, e := client.Do(req)
			var data struct {
				Age     float64 `json:"monitorAgeSeconds"`
				Monitor *struct {
					Disks map[string]struct {
						Available bool `json:"available"`
						Low       bool `json:"low_space"`
					} `json:"disks"`
					GPU struct {
						Available bool `json:"available"`
					} `json:"gpu"`
					Queue struct {
						Available bool    `json:"available"`
						Wait      float64 `json:"oldest_wait_seconds"`
					} `json:"queue"`
				} `json:"monitor"`
			}
			if e != nil {
				current["encoder_unreachable"] = "编码服务不可达，请检查服务状态"
			} else {
				if resp.StatusCode != 200 || json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&data) != nil {
					current["encoder_unreachable"] = "编码服务健康检查失败"
				} else if data.Monitor == nil || data.Age > 180 {
					current["monitor_stale"] = "媒体资源监控缺失或超过3分钟未更新"
				} else {
					for _, label := range []string{"system", "data", "materials"} {
						d, ok := data.Monitor.Disks[label]
						if !ok || !d.Available {
							current["disk_"+label] = "媒体存储不可访问：" + label
						} else if d.Low {
							current["disk_"+label] = "媒体存储可用空间不足：" + label
						}
					}
					if !data.Monitor.GPU.Available {
						current["gpu_monitor"] = "GPU指标无法采集"
					}
					if !data.Monitor.Queue.Available {
						current["queue_monitor"] = "共享队列指标无法采集"
					} else if data.Monitor.Queue.Wait > 300 {
						current["queue_wait"] = "媒体任务排队超过5分钟"
					}
				}
				resp.Body.Close()
			}
			for key, message := range current {
				if !active[key] {
					a.event("warn", "media_alert", message)
				}
			}
			for key := range active {
				if _, ok := current[key]; !ok {
					a.event("info", "media_alert_resolved", "媒体资源告警已恢复："+key)
				}
			}
			active = map[string]bool{}
			for key := range current {
				active[key] = true
			}
		}
		time.Sleep(time.Minute)
	}
}
