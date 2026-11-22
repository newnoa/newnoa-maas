package main

import (
	"errors"
	"runtime"
	"time"
)

func (p *remoteNativeProvider) verifyExitCleanup() error { return verifyRemoteProcessExit(p.owned) }

func verifyRemoteProcessExit(owned remoteOwnedProcess) error {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "windows" {
		return nil
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		alive, err := remoteOwnedProcessAlive(owned)
		if !alive && err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("Owned native process cleanup could not be verified")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Caller holds m.mu; native cleanup must not hold the manager lock.
func (m *remoteManager) providerExitedLocked(s *remoteSession, generation string) {
	provider := m.providers[s.ID]
	s.State = "unknown"
	s.Error = "Native CLI exited; owned process cleanup is pending"
	for _, approval := range s.Approvals {
		if approval.State == "open" {
			approval.State = "invalidated"
		}
	}
	m.changedLocked(s, "", "run.exited", nil)
	if err := m.saveLocked(); err != nil {
		m.setFailureLocked("Remote exit state persistence failed")
	}
	if provider == nil {
		return
	}
	id := s.ID
	go func() {
		err := provider.Close()
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.closed || m.generations[id] != generation || m.providers[id] != provider || m.sessions[id] != s {
			return
		}
		owned, gateway := s.OwnedProcess, s.OwnedGateway
		if err != nil {
			s.State = "unknown"
			s.Error = "Native CLI exited; process cleanup failed. Check the executor status on the host."
		} else {
			delete(m.providers, id)
			s.OwnedProcess = remoteOwnedProcess{}
			s.OwnedGateway = nil
			s.State = "failed"
			s.Error = "Native CLI exited; explicitly resume this native session"
		}
		m.changedLocked(s, "", "run.exitCleanup", nil)
		if saveErr := m.saveLocked(); saveErr != nil {
			m.setFailureLocked("Remote exit cleanup persistence failed")
			m.providers[id] = provider
			s.OwnedProcess = owned
			s.OwnedGateway = gateway
			s.State = "unknown"
			s.Error = m.failure
			m.changedLocked(s, "", "run.exitCleanupPersistenceFailed", nil)
		}
	}()
}
