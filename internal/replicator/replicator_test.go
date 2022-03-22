package replicator

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"github.com/alkur-gh/kmpv2/internal/storage/inmemory"
	"github.com/phayes/freeport"
)

func TestSingleNodePutGetDelete(t *testing.T) {
	c := generateConfig(t)
	defer os.RemoveAll(c.BaseDir)
	c.ID = "node-0"
	c.Bootstrap = true
	r, err := New(c)
	if err != nil {
		t.Fatalf("New(%v) error: %v", c, err)
	}
	waitRaftLeader(t, r)
	want := &api.Record{
		Key:   "key 1",
		Value: []byte("value 1"),
	}
	if err := r.Put(want); err != nil {
		t.Errorf("Put(%v) error: %v", want, err)
	}
	eventually(t, time.Second, 10*time.Millisecond, func() bool {
		got, err := r.Get(want.Key)
		if err != nil {
			t.Errorf("Get(%v) error: %v", want.Key, err)
			return false
		} else if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Get(%v) = %v; want %v", want.Key, got, want)
			return false
		} else {
			return true
		}
	})
	if err := r.Delete(want.Key); err != nil {
		t.Errorf("Delete(%v) error: %v", want.Key, err)
	}
	eventually(t, time.Second, 10*time.Millisecond, func() bool {
		got, err := r.Get(want.Key)
		if _, ok := err.(api.ErrRecordNotFound); ok {
			return true
		} else if err != nil {
			t.Errorf("Get(%v) error: %v", want.Key, err)
		} else {
			t.Errorf("Get(%v) = %v; want ErrRecordNotFound", want.Key, got)
		}
		return false
	})
}

func TestSingleNodeRecovery(t *testing.T) {
	c := generateConfig(t)
	defer os.RemoveAll(c.BaseDir)
	c.ID = "node-0"
	c.Bootstrap = true
	r, err := New(c)
	if err != nil {
		t.Fatalf("New(%v) error: %v", c, err)
	}
	waitRaftLeader(t, r)
	want := &api.Record{
		Key:   "key 1",
		Value: []byte("value 1"),
	}
	if err := r.Put(want); err != nil {
		t.Errorf("Put(%v) error: %v", want, err)
	}
	eventually(t, time.Second, 10*time.Millisecond, func() bool {
		got, err := r.Get(want.Key)
		if err != nil {
			t.Errorf("Get(%v) error: %v", want.Key, err)
			return false
		} else if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Get(%v) = %v; want %v", want.Key, got, want)
			return false
		} else {
			return true
		}
	})

	// make snapshot
	snapFuture := r.raft.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Errorf("raft.Snapshot() error: %v", err)
	}
	snapMeta, reader, err := snapFuture.Open()

	// clear storage
	if err := r.storage.Reset(); err != nil {
		t.Errorf("storage.Reset() error: %v", err)
	}
	_, err = r.Get(want.Key)
	if _, ok := err.(api.ErrRecordNotFound); !ok {
		t.Errorf("Get(%v) expected ErrRecordNotFound", want.Key)
	}

	// restore snapshot
	r.raft.Restore(snapMeta, reader, 1*time.Second)

	// check value
	got, err := r.Get(want.Key)
	if err != nil {
		t.Errorf("Get(%v) error: %v", want.Key, err)
	} else if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
		t.Errorf("Get(%v) = %v; want %v", want.Key, got, want)
	}
}

func TestMultipleNodesPutGetDelete(t *testing.T) {
	const N = 3
	var cfgs []Config
	var rs []*replicator
	for i := 0; i < N; i++ {
		cfg := generateConfig(t)
		cfg.ID = fmt.Sprintf("node-%d", i)
		cfg.Bootstrap = i == 0
		r, err := New(cfg)
		if err != nil {
			t.Fatalf("New(%v) error: %v", cfg, err)
		}
		cfgs = append(cfgs, cfg)
		rs = append(rs, r)
	}
	waitRaftLeader(t, rs[0])
	cluster := []string{cfgs[0].SerfBindAddr}
	for _, r := range rs[1:] {
		if err := r.Join(cluster); err != nil {
			t.Errorf("Join(%v) error: %v", cluster, err)
		}
	}
	want := &api.Record{
		Key:   "key 1",
		Value: []byte("value 1"),
	}
	if err := rs[0].Put(want); err != nil {
		t.Errorf("Put(%v) error: %v", want, err)
	}
	eventually(t, 2*time.Second, 100*time.Millisecond, func() bool {
		for _, r := range rs {
			got, err := r.Get(want.Key)
			if err != nil {
				t.Errorf("Get(%v) error: %v", want.Key, err)
				return false
			} else if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
				t.Errorf("Get(%v) = %v; want %v", want.Key, got, want)
				return false
			}
		}
		return true
	})
	if err := rs[0].Delete(want.Key); err != nil {
		t.Errorf("Delete(%v) error: %v", want.Key, err)
	}
	eventually(t, 2*time.Second, 100*time.Millisecond, func() bool {
		for _, r := range rs {
			got, err := r.Get(want.Key)
			if _, ok := err.(api.ErrRecordNotFound); ok {
				continue
			} else if err != nil {
				t.Errorf("Get(%v) error: %v", want.Key, err)
				return false
			} else {
				t.Errorf("Get(%v) = %v; want ErrRecordNotFound", want.Key, got)
				return false
			}
		}
		return true
	})
}

func generateConfig(t *testing.T) Config {
	t.Helper()
	s, err := inmemory.New(nil)
	if err != nil {
		t.Fatalf("inmemory.New() error: %v", err)
	}
	dir, err := os.MkdirTemp("", "replicator-test-*")
	if err != nil {
		t.Fatalf("os.CreateTemp() error: %v", err)
	}
	err = os.Mkdir(filepath.Join(dir, "raft"), 0777)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("freeport.GetFreePort() error: %v", err)
	}
	serfPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("freeport.GetFreePort() error: %v", err)
	}
	return Config{
		BaseDir:      dir,
		RaftBindAddr: fmt.Sprintf("127.0.0.1:%d", raftPort),
		SerfBindAddr: fmt.Sprintf("127.0.0.1:%d", serfPort),
		Storage:      s,
	}
}

func waitRaftLeader(t *testing.T, r *replicator) {
	eventually(t, 10*time.Second, 100*time.Millisecond, func() bool {
		return r.raft.Leader() != ""
	})
}

func eventually(t *testing.T, timeout, interval time.Duration, f func() bool) {
	t.Helper()
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			t.Errorf("eventually timeout")
			return
		case <-ticker.C:
			if f() {
				return
			}
		}
	}
}