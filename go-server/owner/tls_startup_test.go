package main

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestOwnerTLSStartupChild(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_OWNER_STARTUP") != "1" {
		return
	}
	main()
}

func TestOwnerRejectsPartialTLSBeforeListening(t *testing.T) {
	for _, field := range []string{"NEWNOA_FRP_OWNER_TLS_CERT", "NEWNOA_FRP_OWNER_TLS_KEY"} {
		t.Run(field, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestOwnerTLSStartupChild$")
			command.Env = append(os.Environ(), "NEWNOA_TEST_OWNER_STARTUP=1", "NEWNOA_FRP_OWNER_ID=fixture", "NEWNOA_INTERNAL_TOKEN=fixture-token-01234567890123456789012345", "NEWNOA_EXPECTED_OWNER_SHA256=", "NEWNOA_FRP_OWNER_TLS_CERT=", "NEWNOA_FRP_OWNER_TLS_KEY=", "NEWNOA_FRP_OWNER_BIND=127.0.0.1:0", "NEWNOA_FRP_PLUGIN_BIND=127.0.0.1:0", field+"=/missing-fixture")
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("partial TLS started successfully")
				}
				if !bytes.Contains(output.Bytes(), []byte("owner TLS certificate and key must be configured together")) {
					t.Fatalf("unexpected refusal: %s", output.String())
				}
			case <-time.After(2 * time.Second):
				command.Process.Kill()
				<-done
				t.Fatal("partial TLS did not refuse startup")
			}
		})
	}
}

func TestOwnerRejectsUnreadableTLSBeforePluginBind(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestOwnerTLSStartupChild$")
	command.Env = append(os.Environ(), "NEWNOA_TEST_OWNER_STARTUP=1", "NEWNOA_FRP_OWNER_ID=fixture", "NEWNOA_INTERNAL_TOKEN=fixture-token-01234567890123456789012345", "NEWNOA_EXPECTED_OWNER_SHA256=", "NEWNOA_FRP_OWNER_TLS_CERT=/PRIVATE-missing-cert", "NEWNOA_FRP_OWNER_TLS_KEY=/PRIVATE-missing-key", "NEWNOA_FRP_OWNER_BIND=127.0.0.1:0", "NEWNOA_FRP_PLUGIN_BIND=invalid-plugin-address")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("invalid TLS started")
	}
	if !bytes.Contains(output, []byte("owner TLS certificate/key could not be loaded")) {
		t.Fatalf("TLS must fail before plugin setup: %s", output)
	}
	if bytes.Contains(output, []byte("PRIVATE")) {
		t.Fatal("private paths leaked")
	}
}
