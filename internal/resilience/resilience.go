package resilience

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// CircuitState represents the state of a Circuit Breaker.
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateHalfOpen
	StateOpen
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half_open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// CircuitBreaker protects downstream ticketer endpoints from cascading failures.
type CircuitBreaker struct {
	mu          sync.Mutex
	state       CircuitState
	failures    int
	threshold   int
	resetAfter  time.Duration
	lastFailure time.Time
}

func NewCircuitBreaker(threshold int, resetAfter time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:      StateClosed,
		threshold:  threshold,
		resetAfter: resetAfter,
	}
}

func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateOpen {
		if time.Since(cb.lastFailure) > cb.resetAfter {
			cb.state = StateHalfOpen
			return nil
		}
		return errors.New("circuit breaker is open")
	}
	return nil
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = StateClosed
}

func (cb *CircuitBreaker) RecordFailure() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.threshold {
		cb.state = StateOpen
	}
	return cb.state
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// RetryWithBackoff executes fn with exponential backoff and full jitter.
func RetryWithBackoff(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	var err error
	for i := range attempts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		maxWait := float64(baseDelay) * math.Pow(2, float64(i))
		jitter := time.Duration(rand.Float64() * maxWait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitter):
		}
	}
	return err
}

// ConsistentHashRing shards ticketer domains evenly across worker replicas.
type ConsistentHashRing struct {
	replicas int
	ring     []uint32
	nodes    map[uint32]string
}

func NewConsistentHashRing(nodes []string, virtualReplicas int) *ConsistentHashRing {
	r := &ConsistentHashRing{
		replicas: virtualReplicas,
		nodes:    make(map[uint32]string),
	}
	for _, n := range nodes {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		for i := range virtualReplicas {
			h := hashKey(fmt.Sprintf("%s#%d", n, i))
			r.ring = append(r.ring, h)
			r.nodes[h] = n
		}
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
	return r
}

func (r *ConsistentHashRing) GetNode(key string) string {
	if len(r.ring) == 0 {
		return ""
	}
	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	return r.nodes[r.ring[idx]]
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// ValidateExternalURL prevents SSRF by blocking private, loopback, and link-local IPs.
func ValidateExternalURL(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("only http and https schemes are allowed")
	}
	host := u.Hostname()
	if host == "" || strings.EqualFold(host, "localhost") {
		return errors.New("localhost is not allowed")
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("dns lookup failed: %w", err)
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("access to private/internal IP %s is blocked (SSRF protection)", ip.String())
		}
	}
	return nil
}
