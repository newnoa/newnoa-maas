package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestListenerTLS(t *testing.T) {
	if config, err := listenerTLS("", ""); err != nil || config != nil {
		t.Fatal("default HTTP mode changed")
	}
	for _, paths := range [][2]string{{"missing-cert", ""}, {"", "missing-key"}, {"missing-cert", "missing-key"}} {
		if _, err := listenerTLS(paths[0], paths[1]); err == nil || strings.Contains(err.Error(), "missing-") {
			t.Fatal("invalid TLS must fail without disclosing paths", err)
		}
	}
	fixture := httptest.NewTLSServer(http.NotFoundHandler())
	defer fixture.Close()
	cert := fixture.TLS.Certificates[0]
	folder := t.TempDir()
	certPath := filepath.Join(folder, "cert.pem")
	keyPath := filepath.Join(folder, "key.pem")
	private, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := listenerTLS(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS12 || len(config.Certificates) != 1 {
		t.Fatal("TLS policy missing")
	}
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); io.WriteString(w, "OK") }))
	server.TLS = config
	server.StartTLS()
	defer server.Close()
	trusted := server.Client()
	response, err := trusted.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response, err = http.Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("untrusted certificate accepted")
	}
	wrong := trusted.Transport.(*http.Transport).Clone()
	wrong.TLSClientConfig = wrong.TLSClientConfig.Clone()
	wrong.TLSClientConfig.ServerName = "wrong.example.invalid"
	defer wrong.CloseIdleConnections()
	if response, err = (&http.Client{Transport: wrong}).Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("wrong hostname accepted")
	}
	old := trusted.Transport.(*http.Transport).Clone()
	old.TLSClientConfig = old.TLSClientConfig.Clone()
	old.TLSClientConfig.MinVersion = tls.VersionTLS10
	old.TLSClientConfig.MaxVersion = tls.VersionTLS11
	defer old.CloseIdleConnections()
	if response, err = (&http.Client{Transport: old}).Get(server.URL); err == nil {
		response.Body.Close()
		t.Fatal("obsolete TLS accepted")
	}
	response, err = http.Get(strings.Replace(server.URL, "https:", "http:", 1))
	if err == nil {
		response.Body.Close()
		if response.StatusCode == 200 {
			t.Fatal("plaintext reached handler")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("unverified connection reached handler")
	}
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	private, err = x509.MarshalPKCS8PrivateKey(other)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = listenerTLS(certPath, keyPath); err == nil {
		t.Fatal("mismatched private key accepted")
	}
}

func TestTLSStartupRejectsPartialConfiguration(t *testing.T) {
	if os.Getenv("NEWNOA_TLS_STARTUP_CHILD") == "1" {
		main()
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestTLSStartupRejectsPartialConfiguration$")
	command.Env = []string{"NEWNOA_TLS_STARTUP_CHILD=1", "NEWNOA_INTERNAL_TOKEN=" + strings.Repeat("t", 32), "NEWNOA_GATEWAY_ID=tls-startup-test", "NEWNOA_GATEWAY_BIND=127.0.0.1:0", "NEWNOA_GATEWAY_TLS_CERT_FILE=/private-path-must-not-be-logged"}
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "must be configured together") || strings.Contains(string(output), "listening") || strings.Contains(string(output), "private-path") {
		t.Fatalf("TLS startup did not fail closed: %s", output)
	}
}
