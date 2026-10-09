//go:build !linux && !windows

package main

import "errors"

func recordingFilesystemSpace(path string) (total, free, available uint64, err error) {
	return 0, 0, 0, errors.New("recording filesystem capacity is unsupported on this platform")
}
