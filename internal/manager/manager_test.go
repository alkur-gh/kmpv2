package manager

import (
	"fmt"
	"testing"

	"github.com/phayes/freeport"
)

func TestSetup(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	ports, err := freeport.GetFreePorts(2)
	if err != nil {
		t.Fatalf("GetFreePorts() error: %v", err)
	}
	if err := m.Start(Config{
		ID:        "node-0",
		Bootstrap: true,
		SerfAddr:  fmt.Sprintf("127.0.0.1:%d", ports[0]),
		RaftAddr:  fmt.Sprintf("127.0.0.1:%d", ports[1]),
	}); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	if err := m.Stop(); err != nil {
		t.Errorf("Stop() error: %v", err)
	}
}
