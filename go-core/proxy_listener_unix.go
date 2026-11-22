//go:build !windows

package main

import (
	"net"
	"syscall"
)

func exclusiveProxySocket(_, _ string, _ syscall.RawConn) error { return nil }

func serveReservedProxyListener(listener *net.TCPListener) (net.Listener, error) {
	file, err := listener.File()
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return net.FileListener(file)
}
