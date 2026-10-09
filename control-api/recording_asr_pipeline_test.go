package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewsAudioPrefetchBoundAndOrder(t *testing.T) {
	root := t.TempDir()
	var mu sync.Mutex
	prepared := []float64{}
	second := make(chan struct{})
	consumed := []float64{}
	err := pipelineNewsAudio(context.Background(), 1250, func(ctx context.Context, offset, length float64) (string, error) {
		mu.Lock()
		prepared = append(prepared, offset)
		mu.Unlock()
		if offset == 600 {
			close(second)
		}
		p := filepath.Join(root, fmt.Sprintf("%v.wav", offset))
		return p, os.WriteFile(p, []byte("fixture"), 0600)
	}, func(chunk newsAudioChunk) error {
		if chunk.offset == 0 {
			select {
			case <-second:
			case <-time.After(3 * time.Second):
				t.Fatal("next chunk was not prefetched")
			}
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			count := len(prepared)
			mu.Unlock()
			if count != 2 {
				t.Fatal("more than two chunks prepared")
			}
		}
		consumed = append(consumed, chunk.offset)
		if chunk.offset == 1200 && chunk.length != 50 {
			t.Fatal("last chunk length")
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(consumed, []float64{0, 600, 1200}) {
		t.Fatal(consumed, err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatal("consumed audio not cleaned")
	}
}

func TestNewsAudioFailureCancelsAndJoinsProducer(t *testing.T) {
	second := make(chan struct{})
	joined := make(chan struct{})
	want := errors.New("ASR failed")
	err := pipelineNewsAudio(context.Background(), 1250, func(ctx context.Context, offset, length float64) (string, error) {
		if offset == 600 {
			close(second)
			<-ctx.Done()
			close(joined)
			return "", ctx.Err()
		}
		return "", nil
	}, func(chunk newsAudioChunk) error {
		<-second
		return want
	})
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
	select {
	case <-joined:
	default:
		t.Fatal("producer still running")
	}
}

func TestNewsAudioExtractionFailure(t *testing.T) {
	want := errors.New("extraction failed")
	err := pipelineNewsAudio(context.Background(), 600,
		func(context.Context, float64, float64) (string, error) { return "", want },
		func(newsAudioChunk) error { t.Fatal("invalid audio consumed"); return nil })
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestPrepareNewsAudioRealFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("FFmpeg unavailable")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source.wav")
	err := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency=1000:duration=2", "-ar", "48000", "-ac", "2", source).Run()
	if err != nil {
		t.Fatal(err)
	}
	path, err := prepareNewsAudio(context.Background(), source, root, 0.25, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	duration, err := newsDuration(context.Background(), path)
	if err != nil || duration < 0.49 || duration > 0.51 {
		t.Fatal(duration, err)
	}
	info, err := os.Stat(source)
	if err != nil || info.Size() == 0 {
		t.Fatal("original audio was changed")
	}
}
