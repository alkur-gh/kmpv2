// Package manager wraps replicator and storage for safe operations.
package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"github.com/alkur-gh/kmpv2/internal/replicator"
	"github.com/alkur-gh/kmpv2/internal/storage/inmemory"
)

var (
	ErrStarted    = errors.New("manager already started")
	ErrNotStarted = errors.New("manager not started")
)

type Config struct {
	ID        string   `json:"id"`
	Bootstrap bool     `json:"bootstrap"`
	SerfAddr  string   `json:"serfAddr"`
	RaftAddr  string   `json:"raftAddr"`
	JoinAddrs []string `json:"joinAddrs"`
}

type storage interface {
	replicator.Storage
	GetAll() ([]*api.Record, error)
}

type Manager struct {
	sync.RWMutex
	config     Config
	started    bool
	baseDir    string
	storage    storage
	replicator *replicator.Replicator
}

// New creates new replication manager. The node isn't ready to operate until
// Start method is called.
func New() (*Manager, error) {
	return &Manager{started: false}, nil
}

// Start creates storage and replicator over that storage and uses it
// for subsequent operations.
func (m *Manager) Start(config Config) (err error) {
	m.Lock()
	defer m.Unlock()
	if m.started {
		return ErrStarted
	}
	m.baseDir, err = m.tempDir()
	if err != nil {
		return fmt.Errorf("failed to create data dir: %v", err)
	}
	m.storage, err = inmemory.New(nil)
	if err != nil {
		return err
	}
	m.config = config
	m.replicator, err = replicator.New(replicator.Config{
		ID:           config.ID,
		Bootstrap:    config.Bootstrap,
		RaftBindAddr: config.RaftAddr,
		SerfBindAddr: config.SerfAddr,
		JoinAddrs:    config.JoinAddrs,
		BaseDir:      m.baseDir,
		Storage:      m.storage,
	})
	if err != nil {
		return err
	}
	m.started = true
	return nil
}

type StatusInfo struct {
	Config
	Leader string
	Peers  []string
}

// StatusInfo returns current status of cluster.
func (m *Manager) StatusInfo() (info StatusInfo, err error) {
	m.RLock()
	defer m.RUnlock()
	if !m.started {
		return info, ErrNotStarted
	}
	info.Config = m.config
	info.Leader, err = m.replicator.Leader()
	if err != nil {
		return info, err
	}
	servers, err := m.replicator.Servers()
	if err != nil {
		return info, err
	}
	for _, server := range servers {
		info.Peers = append(info.Peers, fmt.Sprintf("%s (%s)", server.Address, server.ID))
	}
	return info, nil
}

// Put replicates and stores the record.
func (m *Manager) Put(r *api.Record) error {
	m.RLock()
	defer m.RUnlock()
	if !m.started {
		return ErrNotStarted
	}
	return m.replicator.Put(r)
}

// Get returns record with given key directly from storage.
func (m *Manager) Get(key string) (*api.Record, error) {
	m.RLock()
	defer m.RUnlock()
	if !m.started {
		return nil, ErrNotStarted
	}
	return m.storage.Get(key)
}

// GetAll returns all records directly from storage.
func (m *Manager) GetAll() ([]*api.Record, error) {
	m.RLock()
	defer m.RUnlock()
	if !m.started {
		return nil, ErrNotStarted
	}
	return m.storage.GetAll()
}

// Delete deletes node from storage through replicator.
func (m *Manager) Delete(key string) error {
	m.RLock()
	defer m.RUnlock()
	if !m.started {
		return ErrNotStarted
	}
	return m.replicator.Delete(key)
}

// Stop leaves current cluster and cleans up.
func (m *Manager) Stop() error {
	m.Lock()
	defer m.Unlock()
	if !m.started {
		return ErrNotStarted
	}
	if err := m.replicator.Close(); err != nil {
		return err
	}
	m.started = false
	return nil
}

func (m *Manager) tempDir() (string, error) {
	dir, err := os.MkdirTemp("", "replicator-test-*")
	if err != nil {
		return "", err
	}
	err = os.Mkdir(filepath.Join(dir, "raft"), 0777)
	if err != nil {
		return "", err
	}
	return dir, nil
}
