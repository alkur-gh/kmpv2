package replicator

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type raftMembershipHandler struct {
	raft *raft.Raft
}

func (h *raftMembershipHandler) Join(m serf.Member) error {
	future := h.raft.AddVoter(raft.ServerID(m.Name), raft.ServerAddress(m.Tags["raft_addr"]), 0, time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("rmh failed to add voter to raft cluster: %v", err)
	}
	return nil
}

func (h *raftMembershipHandler) Leave(m serf.Member) error {
	future := h.raft.RemoveServer(raft.ServerID(m.Name), 0, time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("rmh failed to remove server from raft cluster: %v", err)
	}
	return nil
}
