package main

import "golang.org/x/sys/unix"

func recordingFilesystemSpace(path string) (total, free, available uint64, err error) {
	var stat unix.Statfs_t
	if err = unix.Statfs(path, &stat); err != nil {
		return
	}
	blockSize := uint64(stat.Bsize)
	return stat.Blocks * blockSize, stat.Bfree * blockSize, stat.Bavail * blockSize, nil
}
