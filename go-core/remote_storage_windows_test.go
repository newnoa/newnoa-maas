package main

import (
	"bytes"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestRemoteJournalWindowsRejectsSharedPermissions(t *testing.T) {
	m, _, _ := remoteFixture(t)
	for range 2 {
		if err := m.saveLocked(); err != nil {
			t.Fatal(err)
		}
		if err := m.load(); err != nil {
			t.Fatal(err)
		}
	}
	sid, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(m.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, acl := range []string{"(A;;FR;;;WD)", "(A;;FA;;;SY)(A;;FR;;;WD)"} {
		sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;" + sid.User.Sid.String() + ")" + acl)
		if err != nil {
			t.Fatal(err)
		}
		dacl, _, err := sd.DACL()
		if err != nil {
			t.Fatal(err)
		}
		if err := windows.SetNamedSecurityInfo(m.path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
			t.Fatal(err)
		}
		if err := m.load(); err == nil {
			t.Fatal("accepted journal readable by Everyone")
		}
		if err := m.saveLocked(); err == nil {
			t.Fatal("overwrote a journal with unexpected access permissions")
		}
		if after, err := os.ReadFile(m.path); err != nil || !bytes.Equal(before, after) {
			t.Fatalf("refused save changed the journal: %v", err)
		}
	}
	// Restore the fixture through the same secure writer so shutdown can clean up.
	if err := os.Remove(m.path); err != nil {
		t.Fatal(err)
	}
}
