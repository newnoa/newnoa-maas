package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

const windowsJobListAttribute = 0x0002000D // PROC_THREAD_ATTRIBUTE_JOB_LIST

func launchRemoteGateway(ctx context.Context, binary, cwd string, args, env []string) (*remoteGatewayProcess, error) {
	if err := remoteCheckAuthority(ctx); err != nil {
		return nil, err
	}
	binary, err := exec.LookPath(binary)
	if err != nil || !filepath.IsAbs(binary) || !strings.EqualFold(filepath.Ext(binary), ".exe") {
		return nil, errors.New("Windows Gateway requires a resolved native executable")
	}
	lifetime := ctx
	if host, ok := ctx.Value(remoteProviderLifetimeKey{}).(context.Context); ok {
		lifetime = host
	}
	life, cancel := context.WithCancel(lifetime)
	job, process, err := startWindowsGatewayProcess(ctx, binary, cwd, args, env)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot start the owned Windows Gateway: %w", err)
	}
	g := &remoteGatewayProcess{done: make(chan struct{}), cancel: cancel}
	identity, err := windowsNativeProcessIdentity(process.Process)
	g.owned = remoteOwnedProcess{PID: int(process.ProcessId), Executable: binary, Identity: identity, Job: remoteProcessJob(int(process.ProcessId))}
	go func() {
		defer close(g.done)
		defer windowsNativeJobs.Delete(int(process.ProcessId))
		defer cancel()
		defer windows.CloseHandle(job)
		defer windows.CloseHandle(process.Process)
		for life.Err() == nil {
			state, waitErr := windows.WaitForSingleObject(process.Process, 50)
			if waitErr != nil {
				g.cleanupErr = waitErr
				break
			}
			if state != uint32(windows.WAIT_TIMEOUT) {
				break
			}
		}
		g.cleanupErr = errors.Join(g.cleanupErr, shutdownWindowsGatewayJob(job))
	}()
	if err != nil {
		return nil, errors.Join(err, g.Close())
	}
	if err = remoteRecordGatewayProcess(ctx, g.owned); err != nil {
		return nil, errors.Join(err, g.Close())
	}
	return g, nil
}

func startWindowsGatewayProcess(ctx context.Context, binary, cwd string, args, env []string) (job windows.Handle, p windows.ProcessInformation, err error) {
	exe, err := atomicWindowsPath(binary)
	if err != nil {
		return 0, p, err
	}
	dir, err := atomicWindowsPath(cwd)
	if err != nil {
		return 0, p, err
	}
	line, err := windows.UTF16FromString(windows.ComposeCommandLine(append([]string{binary}, args...)))
	if err != nil {
		return 0, p, err
	}
	for _, value := range env {
		if strings.IndexByte(value, 0) >= 0 {
			return 0, p, errors.New("invalid Windows Gateway environment")
		}
	}
	block := utf16.Encode([]rune(strings.Join(env, "\x00") + "\x00\x00"))
	var jobName string
	job, jobName, err = newWindowsNativeJob()
	if err != nil {
		return 0, p, err
	}
	defer func(h windows.Handle) {
		if err != nil {
			_ = windows.CloseHandle(h)
		}
	}(job)
	attrs, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return 0, p, err
	}
	defer attrs.Delete()
	// Put the process in the job before its first instruction, without PID-based attachment.
	if err = attrs.Update(windowsJobListAttribute, unsafe.Pointer(&job), unsafe.Sizeof(job)); err != nil {
		return 0, p, err
	}
	si := windows.StartupInfoEx{ProcThreadAttributeList: attrs.List()}
	si.Cb = uint32(unsafe.Sizeof(si))
	if err = remoteCheckAuthority(ctx); err != nil {
		return 0, p, err
	}
	if err = windows.CreateProcess(exe, &line[0], nil, nil, false, windows.CREATE_NO_WINDOW|windows.CREATE_UNICODE_ENVIRONMENT|windows.EXTENDED_STARTUPINFO_PRESENT, &block[0], dir, &si.StartupInfo, &p); err != nil {
		return 0, p, err
	}
	windowsNativeJobs.Store(int(p.ProcessId), jobName)
	_ = windows.CloseHandle(p.Thread)
	return job, p, nil
}

func shutdownWindowsGatewayJob(job windows.Handle) error {
	if err := windows.TerminateJobObject(job, 1); err != nil {
		return err
	}
	// JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, fixed Windows ABI.
	var counts struct {
		times                          [4]int64
		faults, total, active, limited uint32
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&counts)), uint32(unsafe.Sizeof(counts)), nil); err != nil {
			return err
		}
		if counts.active == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("Windows Gateway job still has active processes")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
