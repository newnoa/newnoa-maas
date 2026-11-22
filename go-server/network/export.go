// Package network owns pinned peer transports and bounded native network processes.
// Authentication, admission, settlement and durable evidence belong to the API callers.
package network

import (
	"context"
	"net/http"
)

type Route = peerRoute
type Tunnel = peerTunnel
type Fallback = peerFallback
type EvidenceEvent = peerEvidenceEvent
type Provider = overlayProvider
type Manager = peerOverlayManager
type Connection = peerConnection

func Transport(route Route) (*http.Transport, error) { return peerTransport(route) }
func VerifiedFRPC() (string, error)                  { return verifiedFRPC() }
func StartProvider(ctx context.Context, node string, epoch int64, secret string, port int) (*Provider, error) {
	return startOverlayProvider(ctx, node, epoch, secret, port)
}
func (p *overlayProvider) Handler() http.Handler     { return p.handler }
func (p *overlayProvider) Stop()                     { p.stop() }
func NewManager() *Manager                           { return newPeerOverlayManager() }
func (m *peerOverlayManager) Reset()                 { m.reset() }
func (m *peerOverlayManager) Status() map[string]any { return m.status() }
func (m *peerOverlayManager) Selected(path string)   { m.selected(path) }
func Connect(ctx context.Context, route Route, deployment, key string, record func(*Fallback) error, manager *Manager) (*Connection, error) {
	return connectPeer(ctx, route, deployment, key, record, manager)
}
func ConnectEasyTier(ctx context.Context, route Route, deployment, key string, manager *Manager) (*Connection, error) {
	return connectPeerPolicy(ctx, route, deployment, key, nil, "easytier", manager)
}
func ConnectFRP(ctx context.Context, route Route, deployment, key string, record func(*Fallback) error) (*Connection, error) {
	return connectPeerPolicy(ctx, route, deployment, key, record, "frp")
}
func (c *peerConnection) Client() *http.Client { return c.client }
func (c *peerConnection) URL() string          { return c.url }
func (c *peerConnection) Path() string         { return c.path }
func (c *peerConnection) Close()               { c.close() }
func (c *peerConnection) Fallback() *Fallback  { return c.fallback }
