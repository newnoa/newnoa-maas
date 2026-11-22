package admission

import (
	"strconv"
	"testing"
)

func TestBudgetLimitsAndReleases(t *testing.T) {
	var b Budget
	const host = "192.0.2.1:1234"
	if BodyLimit(-1, 32) != 32 || BodyLimit(33, 32) != 32 || BodyLimit(7, 32) != 7 {
		t.Fatal("incorrect reservation size")
	}
	first := b.Reserve(host, 32<<20)
	second := b.Reserve("[2001:db8::1]:1", 32<<20)
	if first == nil || second == nil || b.Reserve("192.0.2.3:1", 1) != nil || b.Reserve("[::ffff:192.0.2.1]:8", 1) != nil {
		t.Fatal("byte budget not enforced")
	}
	first()
	reused := b.Reserve("[::ffff:192.0.2.1]:8", 1)
	if reused == nil {
		t.Fatal("peer reservation was not returned")
	}
	reused()
	second()
	if b.total != (usage{}) || len(b.peers) != 0 {
		t.Fatal("finished requests retained accounting state")
	}
	var peers Budget
	for i := 0; i < 16; i++ {
		release := peers.Reserve(host, 0)
		if release == nil {
			t.Fatal("available slot rejected")
		}
		defer release()
	}
	if peers.Reserve("192.0.2.1:999", 0) != nil {
		t.Fatal("source port changes must not bypass request budget")
	}
}

func TestBudgetLimitsPerCallerKey(t *testing.T) {
	var b Budget
	var releases []*Reservation
	for i := 0; i < 16; i++ {
		release := b.ReserveFor("192.0.2.1:1234", "caller-a", 0)
		if release == nil {
			t.Fatalf("caller reservation %d rejected", i+1)
		}
		releases = append(releases, release)
	}
	if b.ReserveFor("192.0.2.2:1234", "caller-a", 0) != nil {
		t.Fatal("one API key exceeded its concurrency limit across source IPs")
	}
	other := b.ReserveFor("192.0.2.2:1234", "caller-b", 0)
	if other == nil {
		t.Fatal("independent API key was blocked")
	}
	other.Release()
	for _, release := range releases {
		release.Release()
	}
	if len(b.keys) != 0 {
		t.Fatal("released caller key remained in memory")
	}
}

func TestBudgetSeparatesUploadsFromExecutionSlots(t *testing.T) {
	var b Budget
	active := make([]*Reservation, 64)
	for i := range active {
		remote := "192.0.2." + strconv.Itoa(1+i%9) + ":1234"
		active[i] = b.ReserveFor(remote, "caller-"+strconv.Itoa(i), 1)
		if active[i] == nil || !active[i].Promote() {
			t.Fatalf("execution reservation %d failed", i+1)
		}
	}

	blocked := b.ReserveFor("198.51.100.1:1234", "another-caller", 1)
	if blocked == nil {
		t.Fatal("active execution slots blocked request-body receipt")
	}
	if blocked.Promote() {
		t.Fatal("execution concurrency exceeded its limit")
	}
	active[0].Release()
	if !blocked.Promote() {
		t.Fatal("released execution slot was not reusable")
	}
	blocked.Release()
	for _, reservation := range active[1:] {
		reservation.Release()
	}
	if b.total != (usage{}) || len(b.peers) != 0 || len(b.keys) != 0 {
		t.Fatalf("reservations were not fully released: %#v", b.total)
	}
}
