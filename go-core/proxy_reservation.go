package main

import (
	"context"
	"fmt"
	"net"
)

func reserveProxyListener(port int, allowLAN bool) (net.Listener, error) {
	if port < 1024 || port > 65535 {
		return nil, fmt.Errorf("proxy port must be an integer from 1024 to 65535")
	}
	host := "127.0.0.1"
	if allowLAN {
		host = "0.0.0.0"
	}
	config := net.ListenConfig{Control: exclusiveProxySocket}
	listener, err := config.Listen(context.Background(), "tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("Core cannot start: local proxy port %d is unavailable; choose another port", port)
	}
	return listener, nil
}
