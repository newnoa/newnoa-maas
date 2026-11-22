package main

import "os"

func (g *gateway) ownsNode(owner string) bool {
	local := os.Getenv("NEWNOA_FRP_OWNER_ID")
	return local != "" && owner == local
}
