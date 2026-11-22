package network

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func verifiedFRPC() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	name := "frpc"
	if strings.HasSuffix(executable, ".exe") {
		name += ".exe"
	}
	binary := filepath.Join(filepath.Dir(executable), name)
	manifest, err := os.ReadFile(filepath.Join(filepath.Dir(executable), "frpc-manifest.json"))
	if err != nil {
		return "", errors.New("verified FRP binary is missing; build the Electron test package")
	}
	var m struct {
		SHA string `json:"sha256"`
	}
	digest, err := artifactDigest(binary)
	if err != nil || json.Unmarshal(manifest, &m) != nil || m.SHA != digest {
		return "", errors.New("FRP binary integrity check failed")
	}
	return binary, nil
}

func artifactDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
