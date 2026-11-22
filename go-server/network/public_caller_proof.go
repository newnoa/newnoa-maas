package network

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const PublicCallerProofHeader = "X-Newnoa-Public-Caller-Proof"
const PublicCallerIPHeader = "X-Newnoa-Public-Caller-IP"
const PublicCallerProofTTL = 30 * time.Second

func publicCallerMessage(gateway string, r *http.Request, digest, caller, expires string) ([]byte, error) {
	if gateway == "" || len(gateway) > 80 || r.Method != "POST" || !strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.RawQuery != "" || r.URL.ForceQuery || net.ParseIP(caller) == nil {
		return nil, errors.New("invalid public caller binding")
	}
	for _, name := range []string{"Authorization", "Content-Type", "Idempotency-Key"} {
		if len(r.Header.Values(name)) != 1 || r.Header.Get(name) == "" {
			return nil, errors.New("ambiguous public caller header")
		}
	}
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") || !validAPIKey(strings.TrimPrefix(authorization, "Bearer ")) || strings.HasPrefix(authorization, "Bearer ses_") || strings.HasPrefix(authorization, "Bearer nn_test_") || len(r.Header.Get("Idempotency-Key")) > 128 {
		return nil, errors.New("invalid public caller credential")
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return nil, errors.New("invalid public caller body digest")
	}
	return json.Marshal([]string{"newnoa-public-caller-v1", gateway, r.Method, r.URL.Path, authorization, r.Header.Get("Content-Type"), r.Header.Get("Idempotency-Key"), caller, digest, expires})
}

// The service token stays local. Only a short-lived, destination/request-bound HMAC crosses TLS.
func SignPublicCaller(secret, gateway string, r *http.Request, digest, caller string, now time.Time) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("public caller signing credential unavailable")
	}
	expires := strconv.FormatInt(now.Add(PublicCallerProofTTL).Unix(), 10)
	message, err := publicCallerMessage(gateway, r, digest, caller, expires)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(message)
	return "v1." + expires + "." + digest + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Verify before reading the bounded request body; the caller must compare its existing raw-body hash.
func VerifyPublicCaller(secret, gateway string, r *http.Request, now time.Time) (string, string, error) {
	invalid := errors.New("invalid public caller proof")
	if len(secret) < 32 || len(r.Header.Values(PublicCallerProofHeader)) != 1 || len(r.Header.Values(PublicCallerIPHeader)) != 1 {
		return "", "", invalid
	}
	proof := r.Header.Get(PublicCallerProofHeader)
	if len(proof) > 256 {
		return "", "", invalid
	}
	parts := strings.Split(proof, ".")
	if len(parts) != 4 || parts[0] != "v1" {
		return "", "", invalid
	}
	expires, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || expires < now.Unix() || expires > now.Add(2*PublicCallerProofTTL).Unix() {
		return "", "", invalid
	}
	caller := r.Header.Get(PublicCallerIPHeader)
	message, err := publicCallerMessage(gateway, r, parts[2], caller, parts[1])
	if err != nil {
		return "", "", invalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil {
		return "", "", invalid
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(message)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return "", "", invalid
	}
	return parts[2], caller, nil
}
