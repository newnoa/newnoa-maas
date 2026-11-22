package main

import peernet "github.com/newnoa/newnoa-maas/go-server/network"

// Core consumes only provider mechanics from the pinned shared network module.
type overlayProvider = peernet.Provider
type peerRoute = peernet.Route

var peerTransport = peernet.Transport
var startOverlayProvider = peernet.StartProvider
var verifiedFRPC = peernet.VerifiedFRPC
