package main

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"github.com/google/uuid"
	"golang.org/x/sys/windows"
)

var windowsNativeJobs sync.Map

func remoteProcessJob(pid int) string {
	name, _ := windowsNativeJobs.Load(pid)
	value, _ := name.(string)
	return value
}

func newWindowsNativeJob() (windows.Handle, string, error) {
	owner, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return 0, "", err
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;" + owner.User.Sid.String() + ")(A;;GA;;;SY)")
	if err != nil {
		return 0, "", err
	}
	// Recovery may run from a different Windows login session on the same machine.
	name := `Global\NewNoaRemote-` + uuid.NewString()
	wide, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, "", err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	job, err := windows.CreateJobObject(&sa, wide)
	if err != nil {
		return 0, "", err
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		windows.CloseHandle(job)
		return 0, "", err
	}
	return job, name, nil
}

func startRemoteNativeProcess(cmd *exec.Cmd) (func(), error) {
	job, name, err := newWindowsNativeJob()
	if err != nil {
		return nil, err
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			windows.CloseHandle(job)
			if cmd.Process != nil {
				windowsNativeJobs.Delete(cmd.Process.Pid)
			}
		})
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Suspend before the first instruction so no child can escape job assignment.
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	priorCancel := cmd.Cancel
	if priorCancel != nil {
		cmd.Cancel = func() error { cleanup(); return priorCancel() }
	}
	if err = cmd.Start(); err != nil {
		cleanup()
		return nil, err
	}
	fail := func(err error) (func(), error) {
		cleanup()
		windowsNativeJobs.Delete(cmd.Process.Pid)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(cmd.Process.Pid))
	if err != nil {
		return fail(err)
	}
	if err = windows.AssignProcessToJobObject(job, process); err != nil {
		windows.CloseHandle(process)
		return fail(err)
	}
	if _, err = windowsNativeProcessIdentity(process); err != nil {
		windows.CloseHandle(process)
		return fail(err)
	}
	windowsNativeJobs.Store(cmd.Process.Pid, name)
	if err = resumeWindowsNativeThread(uint32(cmd.Process.Pid)); err != nil {
		windows.CloseHandle(process)
		return fail(err)
	}
	go func() {
		defer windows.CloseHandle(process)
		_, _ = windows.WaitForSingleObject(process, windows.INFINITE)
		cleanup()
	}()
	return cleanup, nil
}

func resumeWindowsNativeThread(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	var threadID uint32
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID == pid {
			if threadID != 0 {
				return errors.New("Suspended Windows process has unexpected threads")
			}
			threadID = entry.ThreadID
		}
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return err
	}
	if threadID == 0 {
		return os.ErrNotExist
	}
	thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, threadID)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(thread)
	count, err := windows.ResumeThread(thread)
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("Unexpected Windows process suspend count")
	}
	return nil
}
