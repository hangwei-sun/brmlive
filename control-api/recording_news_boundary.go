package main

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Only inspect a tiny neighbourhood of the first spoken word. A scene detector
// cannot identify a presenter: it is evidence for a nearby hard cut, not a reason
// to search seconds ahead and accidentally remove the presenter's introduction.
// Audio-only sources and unavailable/uncertain detection keep the speech start.
func alignNewsPicture(ctx context.Context, source string, start, end, floor float64) float64 {
	window := math.Max(0, start-0.4)
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
		"-threads", "2", "-ss", fmt.Sprintf("%.6f", window), "-i", source,
		"-t", "0.800", "-an", "-map", "0:v:0", "-filter_threads", "1",
		"-vf", "scale=320:-2,select='gt(scene,0.30)',metadata=print:file=-",
		"-fps_mode", "vfr", "-f", "null", "-")
	out, err := cmd.Output()
	if err != nil || len(out) > 64*1024 {
		return start
	}
	return chooseNewsPictureStart(start, end, floor, window, string(out))
}

func chooseNewsPictureStart(start, end, floor, window float64, metadata string) float64 {
	best, distance := start, math.Inf(1)
	for _, line := range strings.Split(metadata, "\n") {
		_, value, ok := strings.Cut(line, "pts_time:")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		pts, err := strconv.ParseFloat(fields[0], 64)
		cut := window + pts
		// Allow at most 120 ms of same-shot lead, or 80 ms of timestamp
		// correction. No blind padding, and never include the preceding story.
		if err != nil || math.IsNaN(cut) || math.IsInf(cut, 0) || pts < 0 || cut < floor || cut >= end || cut < start-0.120-1e-6 || cut > start+0.080+1e-6 {
			continue
		}
		if delta := math.Abs(cut - start); delta < distance {
			best, distance = cut, delta
		}
	}
	// Ceil rather than round: millisecond serialization must not move a cut
	// backwards into the last frame of the preceding shot.
	if !math.IsInf(distance, 1) {
		aligned := math.Ceil((best-1e-9)*1000) / 1000
		if aligned < end {
			return aligned
		}
	}
	return start
}
