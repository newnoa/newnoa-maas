package main

// Transport fixtures provide one Server; discovery and binding have separate routing tests.
func fixedGatewayTestCore(home, gateway string) *core {
	c := newCore(home, gateway)
	c.proxyLAN = false // Transport fixtures exercise the explicit loopback credential substitution.
	c.gatewaySelector = nil
	return c
}
