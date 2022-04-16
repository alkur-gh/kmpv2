package membership

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hashicorp/serf/serf"
)

type Config struct {
	NodeName string
	BindAddr string
	Tags     map[string]string
}

type Handler interface {
	Join(serf.Member) error
	Leave(serf.Member) error
}

type membership struct {
	config  Config
	serf    *serf.Serf
	eventCh chan serf.Event
	handler Handler
}

// New creates ready to use membership instance.
func New(handler Handler, config Config) (*membership, error) {
	m := &membership{
		config:  config,
		eventCh: make(chan serf.Event),
		handler: handler,
	}
	if err := m.setupSerf(); err != nil {
		return nil, fmt.Errorf("serf setup error: %v", err)
	}
	go m.eventHandler()
	return m, nil
}

func (m *membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.config.BindAddr)
	if err != nil {
		return fmt.Errorf("ResolveTCPAddr(%v) error: %v", m.config.BindAddr, err)
	}
	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.ReconnectTimeout = 10 * time.Second
	serfConfig.NodeName = m.config.NodeName
	serfConfig.Tags = m.config.Tags
	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	serfConfig.EventCh = m.eventCh
	m.serf, err = serf.Create(serfConfig)
	if err != nil {
		return fmt.Errorf("serf.Create(%v) error: %v", serfConfig, err)
	}
	return nil
}

// Join joins the given cluster.
func (m *membership) Join(cluster []string) error {
	if _, err := m.serf.Join(cluster, true); err != nil {
		return fmt.Errorf("serf.Join(%v) error: %v", cluster, err)
	}
	return nil
}

// Leave leaves current cluster and shutdowns the instance.
// This instance shouldn't be used after this call.
func (m *membership) Leave() error {
	if err := m.serf.Leave(); err != nil {
		return fmt.Errorf("serf.Leave() error: %v", err)
	}
	if err := m.serf.Shutdown(); err != nil {
		return fmt.Errorf("serf.Shutdown() error: %v", err)
	}
	return nil
}

func (m *membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *membership) eventHandler() {
	for e := range m.eventCh {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				if err := m.handleJoin(member); err != nil {
					log.Printf("handleJoin(%v) error: %v", member, err)
				}
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				if err := m.handleLeave(member); err != nil {
					log.Printf("handleLeave(%v) error: %v", member, err)
				}
			}
		}
	}
}

func (m *membership) handleJoin(member serf.Member) error {
	return m.handler.Join(member)
}

func (m *membership) handleLeave(member serf.Member) error {
	return m.handler.Leave(member)
}

func (m *membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}
