package main

import (
	"context"
	"errors"
	"net"
	"time"
)

type safeDialer struct {
	allow   []string
	timeout time.Duration
	lookup  func(context.Context, string) ([]net.IPAddr, error)
	dial    func(context.Context, string, string) (net.Conn, error)
}

func (d safeDialer) DialContext(parent context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(parent, d.timeout)
	defer cancel()
	lookup := d.lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	addresses, err := safeAddresses(ctx, address, d.allow, lookup)
	if err != nil {
		return nil, err
	}
	dial := d.dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	deadline, _ := ctx.Deadline()
	last := errors.New("egress connection unavailable")
	for i, pinned := range addresses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Share one connection budget; HTTP/TLS starts only after a connection is returned.
		attempt, stop := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(addresses)-i))
		connection, err := dial(attempt, network, pinned)
		attemptError := attempt.Err()
		stop()
		if err == nil && attemptError == nil && ctx.Err() == nil {
			return connection, nil
		}
		if connection != nil {
			_ = connection.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			err = attemptError
		}
		last = err
	}
	return nil, last
}
