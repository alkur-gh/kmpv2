package membership

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/phayes/freeport"
)

type mockHandler struct {
	mu     sync.Mutex
	active map[string]serf.Member
}

func (h *mockHandler) Join(m serf.Member) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active[m.Name] = m
	return nil
}

func (h *mockHandler) Leave(m serf.Member) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.active, m.Name)
	return nil
}

func (h *mockHandler) isAlive(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.active[name]
	return ok && m.Status == serf.StatusAlive
}

// TestCluster tests a cluster of multiple nodes for join and leave behavior.
func TestCluster(t *testing.T) {
	const N = 3
	var cfgs []Config
	var ms []*membership
	for i := 0; i < N; i++ {
		cfg := generateConfig(t, fmt.Sprintf("node-%d", i))
		cfgs = append(cfgs, cfg)
		m, err := New(&mockHandler{active: make(map[string]serf.Member)}, cfg)
		if err != nil {
			t.Fatalf("New(%v) error: %v", cfg, err)
		}
		ms = append(ms, m)
	}
	cluster := []string{cfgs[0].BindAddr}
	for _, m := range ms[1:] {
		if err := m.Join(cluster); err != nil {
			t.Errorf("m.Join(%v) error: %v", cluster, err)
		}
	}
	eventually(t, 2*time.Second, 100*time.Millisecond, func() bool {
		for _, m := range ms {
			if countMembers(m.serf.Members(), serf.StatusAlive) != N {
				return false
			}
		}
		return true
	})
	for i := 0; i < N; i += 2 {
		if err := ms[i].Leave(); err != nil {
			t.Errorf("m.Leave() error: %v", err)
		}
	}
	eventually(t, 2*time.Second, 100*time.Millisecond, func() bool {
		for i := 1; i < N; i += 2 {
			if countMembers(ms[i].serf.Members(), serf.StatusAlive) != N/2 {
				return false
			}
		}
		return true
	})
}

func generateConfig(t *testing.T, nodeName string) Config {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("freeport.GetFreePort() error: %v", err)
	}
	return Config{
		NodeName: nodeName,
		BindAddr: fmt.Sprintf("127.0.0.1:%d", port),
	}
}

func eventually(t *testing.T, timeout, interval time.Duration, f func() bool) {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			t.Error("eventually timeout")
			return
		case <-ticker.C:
			if f() {
				return
			}
		}
	}
}

func countMembers(members []serf.Member, status serf.MemberStatus) int {
	count := 0
	for _, m := range members {
		if m.Status == status {
			count++
		}
	}
	return count
}
