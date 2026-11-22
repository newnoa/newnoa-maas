package main

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

type grpcGatewayIdentity struct{ id, address, transport, ca, dial string }

type grpcGatewayEntry struct {
	connection *grpc.ClientConn
	users      int
	idle       *time.Timer
	generation uint64
}

type grpcGatewayPool struct {
	mu          sync.Mutex
	entries     map[grpcGatewayIdentity]*grpcGatewayEntry
	idleTimeout time.Duration
}

var gatewayGRPCConnections = grpcGatewayPool{idleTimeout: time.Minute}

func (p *grpcGatewayPool) acquire(endpoint gatewayEndpoint) (*grpc.ClientConn, func(), error) {
	key := grpcGatewayIdentity{endpoint.ID, endpoint.Address, endpoint.Transport, endpoint.TrustedCA, endpoint.dialAddress}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries == nil {
		p.entries = make(map[grpcGatewayIdentity]*grpcGatewayEntry)
	}
	entry := p.entries[key]
	if entry == nil {
		connection, err := newGRPCGatewayConnection(endpoint)
		if err != nil {
			return nil, nil, err
		}
		if len(p.entries) >= gatewayMaxNodes {
			for oldKey, old := range p.entries {
				if old.users == 0 {
					old.idle.Stop()
					old.connection.Close()
					delete(p.entries, oldKey)
					break
				}
			}
			if len(p.entries) >= gatewayMaxNodes {
				// All cached connections are busy; preserve admission and close this request's connection.
				var once sync.Once
				return connection, func() { once.Do(func() { connection.Close() }) }, nil
			}
		}
		entry = &grpcGatewayEntry{connection: connection}
		p.entries[key] = entry
	}
	if entry.idle != nil {
		entry.idle.Stop()
	}
	entry.generation++
	entry.users++
	var once sync.Once
	return entry.connection, func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			entry.users--
			if entry.users != 0 {
				return
			}
			generation := entry.generation
			entry.idle = time.AfterFunc(p.idleTimeout, func() {
				p.mu.Lock()
				if p.entries[key] != entry || entry.users != 0 || entry.generation != generation {
					p.mu.Unlock()
					return
				}
				delete(p.entries, key)
				p.mu.Unlock()
				entry.connection.Close()
			})
		})
	}, nil
}

func (p *grpcGatewayPool) closeIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.entries {
		if entry.users == 0 {
			entry.idle.Stop()
			entry.connection.Close()
			delete(p.entries, key)
		}
	}
}
