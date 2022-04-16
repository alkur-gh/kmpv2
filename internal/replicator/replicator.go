package replicator

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"github.com/alkur-gh/kmpv2/internal/membership"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/protobuf/proto"
)

type Storage interface {
	Put(*api.Record) error
	Get(string) (*api.Record, error)
	Delete(string) error
	Reset() error
	Save(io.Writer) error
	Load(io.Reader) error
}

type Membership interface {
	Join([]string) error
	Leave() error
	Members() []serf.Member
}

type Config struct {
	ID           string
	Bootstrap    bool
	BaseDir      string
	RaftBindAddr string
	SerfBindAddr string
	JoinAddrs    []string
	Storage      Storage
}

type Replicator struct {
	config          Config
	storage         Storage
	raft            *raft.Raft
	membership      Membership
	membershipMutex sync.Mutex
}

// New returns new ready to use replicator.
func New(config Config) (*Replicator, error) {
	var err error
	r := &Replicator{
		config:  config,
		storage: config.Storage,
	}
	err = r.setupRaft()
	if err != nil {
		return nil, fmt.Errorf("setupRaft: %v", err)
	}
	err = r.setupMembership()
	if err != nil {
		return nil, fmt.Errorf("setupMembership: %v", err)
	}
	go r.syncRaftAndSerf()
	return r, nil
}

func (r *Replicator) syncRaftAndSerf() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for _ = range ticker.C {
		if r.raft.State() == raft.Shutdown {
			break
		} else if r.raft.State() != raft.Leader {
			continue
		}
		raftServers := make(map[string]string)
		if srvs, err := r.Servers(); err != nil {
			fmt.Fprintf(os.Stderr, "syncRaftAndSerf: %v", err)
			continue
		} else {
			for _, s := range srvs {
				raftServers[string(s.ID)] = string(s.Address)
			}
		}
		serfServers := make(map[string]string)
		for _, m := range r.membership.Members() {
			if m.Status == serf.StatusAlive {
				serfServers[m.Name] = m.Tags["raft_addr"]
			}
		}
		for id, addr := range serfServers {
			if _, ok := raftServers[id]; !ok {
				future := r.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, time.Second)
				if err := future.Error(); err != nil {
					fmt.Fprintf(os.Stderr, "syncRaftAndSerf: failed to add voter: %v", err)
				}
			}
		}
		for id, _ := range raftServers {
			if _, ok := serfServers[id]; !ok {
				future := r.raft.RemoveServer(raft.ServerID(id), 0, time.Second)
				if err := future.Error(); err != nil {
					fmt.Fprintf(os.Stderr, "syncRaftAndSerf: failed to remove voter: %v", err)
				}
			}
		}
		//println("sync: tick")
		//fmt.Printf("[%s] raft nodes: %s\n", r.config.ID, raftServers)
		//fmt.Printf("[%s] serf nodes: %s\n", r.config.ID, serfServers)
	}
}

func (r *Replicator) setupRaft() error {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(r.config.ID)
	dir := filepath.Join(r.config.BaseDir, "raft")
	logs, err := boltdb.NewBoltStore(filepath.Join(dir, "logs"))
	if err != nil {
		return fmt.Errorf("bolt store for logs: %v", err)
	}
	stable, err := boltdb.NewBoltStore(filepath.Join(dir, "stable"))
	if err != nil {
		return fmt.Errorf("bolt store for stable data: %v", err)
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dir, "fss"), 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %v", err)
	}
	trans, err := raft.NewTCPTransport(r.config.RaftBindAddr, nil, 1, time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft transport: %v", err)
	}
	r.raft, err = raft.NewRaft(
		c,      // *raft.Config
		r,      // raft.FSM
		logs,   // raft.LogStore
		stable, // raft.StableStore
		snaps,  // raft.SnapshotStore
		trans,  // raft.Transport
	)
	if err != nil {
		return fmt.Errorf("new raft: %v", err)
	}
	if r.config.Bootstrap {
		err := r.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(r.config.ID),
					Address:  raft.ServerAddress(r.config.RaftBindAddr),
				},
			},
		}).Error()
		if err != nil {
			return fmt.Errorf("raft bootstrap cluster: %v", err)
		}
	}
	return nil
}

func (r *Replicator) setupMembership() error {
	var err error
	r.membership, err = membership.New(&raftMembershipHandler{r.raft}, membership.Config{
		NodeName: r.config.ID,
		BindAddr: r.config.SerfBindAddr,
		Tags: map[string]string{
			"raft_addr": r.config.RaftBindAddr,
		},
	})
	if err != nil {
		return err
	}
	if !r.config.Bootstrap {
		return r.membership.Join(r.config.JoinAddrs)
	}
	return nil
}

// Close cleans up and shutdowns the replicator.
func (r *Replicator) Close() error {
	if err := r.membership.Leave(); err != nil {
		return err
	}
	if err := r.raft.Shutdown().Error(); err != nil {
		return err
	}
	return nil
}

// Servers returns servers participating in replication.
func (r *Replicator) Servers() ([]raft.Server, error) {
	future := r.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	conf := future.Configuration()
	return conf.Servers, nil
}

// Leader returns id of current replication leader.
func (r *Replicator) Leader() (string, error) {
	return string(r.raft.Leader()), nil
}

const (
	putCode    = 0
	deleteCode = 1
)

// Put replicates the put command and returns result of applying it on storage.
func (r *Replicator) Put(rec *api.Record) error {
	recBytes, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	bytes := make([]byte, 1+len(recBytes))
	bytes[0] = putCode
	copy(bytes[1:], recBytes)
	future := r.raft.Apply(bytes, time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	resp := future.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

// Get returns record stored in current node.
func (r *Replicator) Get(key string) (*api.Record, error) {
	return r.storage.Get(key)
}

// Delete replicates the delete command and returns result of applying the command.
func (r *Replicator) Delete(key string) error {
	keyBytes := []byte(key)
	bytes := make([]byte, 1+len(keyBytes))
	bytes[0] = deleteCode
	copy(bytes[1:], keyBytes)
	future := r.raft.Apply(bytes, time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	resp := future.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

// Apply implements raft.FSM.Apply.
func (r *Replicator) Apply(entry *raft.Log) interface{} {
	if len(entry.Data) < 1 {
		return fmt.Errorf("expected non empty data")
	}
	switch entry.Data[0] {
	case putCode:
		var rec api.Record
		if err := proto.Unmarshal(entry.Data[1:], &rec); err != nil {
			return fmt.Errorf("replicator apply put unmarshal: %v", err)
		}
		return r.storage.Put(&rec)
	case deleteCode:
		key := string(entry.Data[1:])
		return r.storage.Delete(key)
	}
	return nil
}

// Snapshot implements raft.FSM.Snapshot.
func (r *Replicator) Snapshot() (raft.FSMSnapshot, error) {
	return r, nil
}

// Persist implements raft.FSMSnapshot.Persist.
func (r *Replicator) Persist(sink raft.SnapshotSink) error {
	if err := r.storage.Save(sink); err != nil {
		return sink.Cancel()
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.Release.
func (r *Replicator) Release() {}

// Restore implements raft.FSM.Restore.
func (r *Replicator) Restore(snapshot io.ReadCloser) error {
	if err := r.storage.Load(snapshot); err != nil {
		snapshot.Close()
		return err
	}
	return snapshot.Close()
}
