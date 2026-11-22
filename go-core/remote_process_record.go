package main

import (
	"context"
	"errors"
	"path/filepath"
)

type remoteProcessOwnerKey struct{}
type remoteGatewayOwnerKey struct{}
type remoteProviderSessionSlotKey struct{}

func remoteRecordGatewayProcess(ctx context.Context, p remoteOwnedProcess) error {
	if record, ok := ctx.Value(remoteGatewayOwnerKey{}).(func(remoteOwnedProcess) error); ok {
		return record(p)
	}
	return nil
}

func remoteRecordOwnedProcess(ctx context.Context, p remoteOwnedProcess) error {
	if record, ok := ctx.Value(remoteProcessOwnerKey{}).(func(remoteOwnedProcess) error); ok {
		return record(p)
	}
	return nil
}
func (m *remoteManager) providerContext(operation context.Context, id, generation string) context.Context {
	ctx := remoteProviderContext(m.ctx, operation)
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, filepath.Dir(m.path))
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, id)
	record := func(p remoteOwnedProcess, gateway bool) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.closed || m.generations[id] != generation {
			return errors.New("Remote native owner changed during launch")
		}
		s := m.sessions[id]
		if s == nil {
			return errors.New("Remote native session was removed")
		}
		if gateway {
			s.OwnedGateway = &p
		} else {
			s.OwnedProcess = p
		}
		return m.saveLocked()
	}
	ctx = context.WithValue(ctx, remoteGatewayOwnerKey{}, func(p remoteOwnedProcess) error { return record(p, true) })
	return context.WithValue(ctx, remoteProcessOwnerKey{}, func(p remoteOwnedProcess) error { return record(p, false) })
}
