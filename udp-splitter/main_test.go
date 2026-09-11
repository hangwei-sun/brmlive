package main

import (
	"strings"
	"testing"
)

func TestFFmpegArgsReadsEachMPTSOnce(t *testing.T) {
	args := ffmpegArgs(group{Address: "224.2.2.1", Port: 10002, Programs: []program{{ProgramNumber: 256, PublishPath: "audio_encoder_1"}, {ProgramNumber: 512, PublishPath: "audio_encoder_2"}}}, "rtmp://127.0.0.1:1935")
	joined := strings.Join(args, " ")
	if strings.Count(joined, "udp://@224.2.2.1:10002") != 1 {
		t.Fatalf("expected one multicast input: %s", joined)
	}
	if !strings.Contains(joined, "0:p:256") || !strings.Contains(joined, "0:p:512") {
		t.Fatalf("program maps missing: %s", joined)
	}
	if strings.Count(joined, "-f flv") != 2 {
		t.Fatalf("expected two outputs: %s", joined)
	}
}

func TestFFmpegArgsSupportsSingleUDP(t *testing.T) {
	args := ffmpegArgs(group{Mode: "single", InputURL: "udp://@192.0.2.20:12000", InputFormat: "mpegts", Programs: []program{{PublishPath: "audio_single"}}}, "rtmp://127.0.0.1:1935")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-f mpegts") || !strings.Contains(joined, "udp://@192.0.2.20:12000") {
		t.Fatalf("single UDP input missing: %s", joined)
	}
	if !strings.Contains(joined, "-map 0:a:0?") || strings.Contains(joined, "0:p:") {
		t.Fatalf("single UDP should select first audio track: %s", joined)
	}
	if strings.Count(joined, "audio_single") != 1 {
		t.Fatalf("single UDP should have one output: %s", joined)
	}
}

func TestFFmpegInputFormatAliases(t *testing.T) {
	if got := ffmpegInputFormat("adts"); got != "aac" {
		t.Fatalf("expected adts alias to aac, got %q", got)
	}
	if got := ffmpegInputFormat("mpegts"); got != "mpegts" {
		t.Fatalf("unexpected mpegts format: %q", got)
	}
}
