package main

import (
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func exclusiveProxySocket(_, _ string, socket syscall.RawConn) error {
	const soExclusiveAddrUse = -5 // Winsock SO_EXCLUSIVEADDRUSE.
	var sockErr error
	if err := socket.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, soExclusiveAddrUse, 1)
	}); err != nil {
		return err
	}
	return sockErr
}

// Serve gets a stoppable view because duplicating an active Winsock listener can fail.
func serveReservedProxyListener(listener *net.TCPListener) (net.Listener, error) {
	return &proxyListenerView{TCPListener: listener}, nil
}

type proxyListenerView struct {
	*net.TCPListener
	accept sync.Mutex
	closed atomic.Bool
	once   sync.Once
}

func (l *proxyListenerView) Accept() (net.Conn, error) {
	l.accept.Lock()
	defer l.accept.Unlock()
	if l.closed.Load() {
		return nil, net.ErrClosed
	}
	conn, err := l.TCPListener.Accept()
	if l.closed.Load() {
		if conn != nil {
			conn.Close()
		}
		return nil, net.ErrClosed
	}
	return conn, err
}

func (l *proxyListenerView) Close() error {
	l.once.Do(func() {
		l.closed.Store(true)
		// Wake the old Accept before letting the next Serve use this socket.
		_ = l.TCPListener.SetDeadline(time.Now())
		l.accept.Lock()
		defer l.accept.Unlock()
		_ = l.TCPListener.SetDeadline(time.Time{})
	})
	return nil
}
