package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"

	"github.com/google/uuid"

	"golang.org/x/sys/windows"
)

func remoteProcessIdentity(pid int) (string, error) {
	if pid <= 1 || uint64(pid) > uint64(^uint32(0)) {
		return "", errors.New("Invalid owned process identity")
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		return "", os.ErrNotExist
	}
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	return windowsNativeProcessIdentity(handle)
}

func windowsNativeProcessIdentity(handle windows.Handle) (string, error) {
	state, err := windows.WaitForSingleObject(handle, 0)
	if err != nil {
		return "", err
	}
	if state == windows.WAIT_OBJECT_0 {
		return "", os.ErrNotExist
	}
	if state != uint32(windows.WAIT_TIMEOUT) {
		return "", errors.New("Windows process state could not be verified")
	}
	var token windows.Token
	if err = windows.OpenProcessToken(handle, windows.TOKEN_QUERY, &token); err != nil {
		return "", err
	}
	defer token.Close()
	owner, err := token.GetTokenUser()
	if err != nil {
		return "", err
	}
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", err
	}
	if !owner.User.Sid.Equals(current.User.Sid) {
		return "", errors.New("Windows process belongs to another user")
	}
	var birth, exit, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &birth, &exit, &kernel, &user); err != nil {
		return "", err
	}
	if birth.HighDateTime == 0 && birth.LowDateTime == 0 {
		return "", errors.New("Windows process birth could not be verified")
	}
	return fmt.Sprintf("windows:%s/%08x%08x:native", owner.User.Sid.String(), birth.HighDateTime, birth.LowDateTime), nil
}

func remoteOwnedProcessAlive(p remoteOwnedProcess) (bool, error) {
	job, alive, err := openOwnedWindowsJob(p, 0x0004)
	if job != 0 {
		windows.CloseHandle(job)
	}
	return alive, err
}

func stopRemoteOwnedProcess(p remoteOwnedProcess) error {
	job, alive, err := openOwnedWindowsJob(p, 0x0004|0x0008)
	if job != 0 {
		defer windows.CloseHandle(job)
	}
	if err != nil || !alive {
		return err
	}
	return shutdownWindowsGatewayJob(job)
}

var openWindowsJob = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
var isWindowsProcessInJob = windows.NewLazySystemDLL("kernel32.dll").NewProc("IsProcessInJob")

func openOwnedWindowsJob(p remoteOwnedProcess, access uint32) (windows.Handle, bool, error) {
	const prefix = `Global\NewNoaRemote-`
	if p.PID <= 1 || !strings.HasPrefix(p.Job, prefix) || !strings.HasPrefix(p.Identity, "windows:S-1-") || !strings.HasSuffix(p.Identity, ":native") {
		return 0, false, errors.New("Legacy Windows process tree ownership requires manual verification")
	}
	if _, err := uuid.Parse(strings.TrimPrefix(p.Job, prefix)); err != nil {
		return 0, false, err
	}
	name, err := windows.UTF16PtrFromString(p.Job)
	if err != nil {
		return 0, false, err
	}
	result, _, callErr := openWindowsJob.Call(uintptr(access), 0, uintptr(unsafe.Pointer(name)))
	if result == 0 {
		if errors.Is(callErr, windows.ERROR_FILE_NOT_FOUND) {
			return 0, false, nil
		}
		return 0, false, callErr
	}
	job := windows.Handle(result)
	var counts struct {
		times                          [4]int64
		faults, total, active, limited uint32
	}
	if err = windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&counts)), uint32(unsafe.Sizeof(counts)), nil); err != nil {
		return job, false, err
	}
	if counts.active == 0 {
		return job, false, nil
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(p.PID))
	if err != nil {
		return job, true, err
	}
	defer windows.CloseHandle(process)
	identity, err := windowsNativeProcessIdentity(process)
	if err != nil {
		return job, true, err
	}
	if identity != p.Identity {
		return job, true, errors.New("Owned Windows process birth identity changed")
	}
	var belongs int32
	result, _, callErr = isWindowsProcessInJob.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&belongs)))
	if result == 0 {
		return job, true, callErr
	}
	if belongs == 0 {
		return job, true, errors.New("Windows process is not in the recorded job")
	}
	return job, true, nil
}
