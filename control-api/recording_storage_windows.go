package main

import "golang.org/x/sys/windows"

func recordingFilesystemSpace(path string) (total, free, available uint64, err error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, 0, err
	}
	err = windows.GetDiskFreeSpaceEx(name, &available, &total, &free)
	return
}
