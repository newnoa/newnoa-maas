package network

import (
	"os"
	"path/filepath"
	"testing"
)

func TestArtifactDigestRereadsSameSizeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native")
	for _, body := range []string{"", "abc", "cba"} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := artifactDigest(path)
		if err != nil || got != hash([]byte(body)) {
			t.Fatalf("digest=%q error=%v", got, err)
		}
	}
	if _, err := artifactDigest(path + "-missing"); err == nil {
		t.Fatal("missing executable accepted")
	}
	if _, err := artifactDigest(filepath.Dir(path)); err == nil {
		t.Fatal("directory accepted")
	}
}

func BenchmarkArtifactDigest24MiB(b *testing.B) {
	path := filepath.Join(b.TempDir(), "native")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err = f.Truncate(24 << 20); err != nil {
		f.Close()
		b.Fatal(err)
	}
	if err = f.Close(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(24 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := artifactDigest(path); err != nil {
			b.Fatal(err)
		}
	}
}
