package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/google/uuid"
	"golang.org/x/sys/windows"
)

func checkRemoteJournalPrivacy(path, uid string, _ os.FileInfo) error {
	name, err := windowsFilePath(path)
	if err != nil {
		return err
	}
	sd, err := windows.GetNamedSecurityInfo(name, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	if sd == nil {
		return errors.New("remote state file has no Windows security descriptor")
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil || owner.String() != uid {
		return errors.New("remote state file must be owned by the current Windows user")
	}
	dacl, _, err := sd.DACL()
	control, _, controlErr := sd.Control()
	if err != nil || controlErr != nil || dacl == nil || control&windows.SE_DACL_PROTECTED == 0 || dacl.AceCount != 2 {
		return errors.New("remote state file must have a private Windows ACL")
	}
	var user, system bool
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil || ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Header.AceFlags != 0 {
			return errors.New("remote state file has unexpected Windows ACL entries")
		}
		// ACCESS_ALLOWED_ACE carries a native SID starting at SidStart.
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		user = user || sid.Equals(owner)
		system = system || sid.IsWellKnown(windows.WinLocalSystemSid)
	}
	runtime.KeepAlive(sd)
	if !user || !system {
		return errors.New("remote state file must grant access only to its Windows owner and SYSTEM")
	}
	return nil
}

func writeRemoteJournal(path, uid string, data []byte) error {
	owner, err := windows.StringToSid(uid)
	if err != nil {
		return err
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;" + owner.String() + ")(A;;FA;;;SY)")
	if err != nil {
		return err
	}
	return writeAtomicFile(path, data, func(dir string) (*os.File, error) {
		temp := filepath.Join(dir, ".newnoa-"+uuid.NewString())
		name, err := atomicWindowsPath(temp)
		if err != nil {
			return nil, err
		}
		// Set the DACL at creation; opening first would leave an inherited-access window.
		sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
		h, err := windows.CreateFile(name, windows.GENERIC_WRITE, 0, &sa, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
		if err != nil {
			return nil, &os.PathError{Op: "create private file", Path: temp, Err: err}
		}
		return os.NewFile(uintptr(h), temp), nil
	})
}
