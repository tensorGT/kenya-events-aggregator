package resilience

import (
	"testing"
	"time"
)

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected closed breaker, got %v", err)
	}
	cb.RecordFailure()
	cb.RecordFailure()
	if err := cb.Allow(); err == nil {
		t.Fatal("expected open breaker to reject")
	}
	time.Sleep(60 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected half-open breaker to allow, got %v", err)
	}
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected closed state, got %v", cb.State())
	}
}

func TestConsistentHashRing(t *testing.T) {
	ring := NewConsistentHashRing([]string{"worker-1", "worker-2"}, 32)
	n := ring.GetNode("mytickets.africa")
	if n != "worker-1" && n != "worker-2" {
		t.Fatalf("unexpected node %q", n)
	}
}

func TestSSRFGuard(t *testing.T) {
	for _, bad := range []string{
		"http://localhost:8080/test",
		"http://127.0.0.1/admin",
		"http://169.254.169.254/latest/meta-data/",
		"ftp://example.com/file",
	} {
		if err := ValidateExternalURL(bad); err == nil {
			t.Fatalf("expected SSRF error for %q", bad)
		}
	}
}
