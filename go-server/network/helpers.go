package network

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
)

const maxFrame = 1 << 20

var nodePattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

func hash(raw []byte) string { digest := sha256.Sum256(raw); return hex.EncodeToString(digest[:]) }
func randomToken() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)
}

var credentialPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,256}$`)

func validAPIKey(value string) bool { return credentialPattern.MatchString(value) }
