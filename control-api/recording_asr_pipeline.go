package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type newsAudioChunk struct {
	path           string
	offset, length float64
	extractSeconds float64
	err            error
}

// At most two prepared/in-flight chunks: current ASR + next extraction.
// Cancellation joins the producer before its isolated temporary directory is
// removed, so no FFmpeg process can recreate files after cleanup.
func pipelineNewsAudio(parent context.Context, duration float64,
	prepare func(context.Context, float64, float64) (string, error),
	consume func(newsAudioChunk) error) error {
	ctx, cancel := context.WithCancel(parent)
	chunks := make(chan newsAudioChunk, 1)
	slots := make(chan struct{}, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(chunks)
		for offset := 0.0; offset < duration; offset += 600 {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			length := min(600.0, duration-offset)
			started := time.Now()
			path, err := prepare(ctx, offset, length)
			chunk := newsAudioChunk{path: path, offset: offset, length: length, extractSeconds: time.Since(started).Seconds(), err: err}
			select {
			case chunks <- chunk:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				return nil
			}
			if chunk.err != nil {
				return chunk.err
			}
			err := consume(chunk)
			_ = os.Remove(chunk.path)
			<-slots
			if err != nil {
				return err
			}
		}
	}
}

func prepareNewsAudio(ctx context.Context, source, root string, offset, length float64) (string, error) {
	path := filepath.Join(root, fmt.Sprintf("audio-%06d.wav", int(offset)))
	cmd := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-threads", "2", "-ss", fmt.Sprintf("%.3f", offset), "-i", source, "-t", fmt.Sprintf("%.3f", length),
		"-map", "0:a:0", "-vn", "-sn", "-dn", "-filter_threads", "1", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", path)
	return path, cmd.Run()
}
