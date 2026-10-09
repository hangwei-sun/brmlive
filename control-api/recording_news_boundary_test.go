package main

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNewsPictureStartSafety(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pts   string
		floor float64
		want  float64
	}{
		{"late cut removes old frames", "0.46", 0, 10.06},
		{"early cut includes only new shot", "0.30", 0, 9.9},
		{"no blind padding", "", 0, 10},
		{"distant cut cannot remove introduction", "0.60", 0, 10},
		{"previous story cannot be included", "0.30", 9.95, 10},
		{"invalid timestamp", "NaN", 0, 10},
		{"ceil frame timestamp", "0.433367", 0, 10.034},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := chooseNewsPictureStart(10, 20, tc.floor, 9.6, "frame:1 pts:1 pts_time:"+tc.pts)
			if math.Abs(got-tc.want) > 1e-6 {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestNewsPictureStartRealFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	for _, fps := range []int{25, 30} {
		t.Run(fmt.Sprint(fps), func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "two-shots.mp4")
			// Hard cut at 2 seconds from blue (previous report) to red (presenter).
			cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
				fmt.Sprintf("color=red:s=160x90:r=%d:d=4", fps), "-vf", "drawbox=color=blue:t=fill:enable='lt(t,2)'",
				"-c:v", "libx264", "-threads", "2", source)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("fixture: %v %s", err, out)
			}
			start := alignNewsPicture(context.Background(), source, 1.94, 4, 0)
			if math.Abs(start-2) > 0.001 {
				t.Fatalf("old frames not removed: %v", start)
			}
			// Use the same input seek and re-encode as the production exporter;
			// decode its first frame, rather than merely trusting the timestamp.
			export := filepath.Join(t.TempDir(), "news.mp4")
			out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", fmt.Sprintf("%.3f", start),
				"-i", source, "-t", "0.5", "-c:v", "libx264", "-threads", "2", export).CombinedOutput()
			if err != nil {
				t.Fatalf("export: %v %s", err, out)
			}
			pixel, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", export, "-frames:v", "1",
				"-vf", "scale=1:1", "-pix_fmt", "rgb24", "-f", "rawvideo", "-").Output()
			if err != nil || len(pixel) != 3 || pixel[0] < 180 || pixel[2] > 60 {
				t.Fatalf("first frame contains previous shot: %v %v", pixel, err)
			}
			if got := alignNewsPicture(context.Background(), source, 2.5, 4, 0); got != 2.5 {
				t.Fatalf("no cut should preserve speech: %v", got)
			}
		})
	}
}
