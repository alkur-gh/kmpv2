package inmemory

import (
	"fmt"
	"io"
	"log"
	"sync"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"google.golang.org/protobuf/proto"
)

type storage struct {
	sync.RWMutex
	m map[string]*api.Record
	l *log.Logger
}

// New returns new empty records storage.
func New(logger *log.Logger) (*storage, error) {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	s := &storage{
		m: make(map[string]*api.Record),
		l: logger,
	}
	return s, nil
}

// Put adds the record.
func (s *storage) Put(r *api.Record) error {
	s.Lock()
	defer s.Unlock()
	s.l.Printf("Put(%v)", r)
	s.m[r.Key] = r
	return nil
}

// Get returns record associated with the given key.
// If record not found, ErrRecordNotFound error is returned.
func (s *storage) Get(key string) (*api.Record, error) {
	s.RLock()
	defer s.RUnlock()
	s.l.Printf("Get(%v)", key)
	r, ok := s.m[key]
	if !ok {
		return nil, api.ErrRecordNotFound{Key: key}
	}
	return r, nil
}

// Delete deletes record associated with the given key.
func (s *storage) Delete(key string) error {
	s.Lock()
	defer s.Unlock()
	s.l.Printf("Delete(%v)", key)
	delete(s.m, key)
	return nil
}

// Reset resets the storage.
func (s *storage) Reset() error {
	s.Lock()
	defer s.Unlock()
	s.l.Printf("Reset()")
	s.m = make(map[string]*api.Record)
	return nil
}

func (s *storage) GetAll() ([]*api.Record, error) {
	s.l.Printf("GetAll()")
	s.RLock()
	defer s.RUnlock()
	var rr []*api.Record
	for _, r := range s.m {
		rr = append(rr, r)
	}
	return rr, nil
}

// Save serializes storage into the given writer.
// Inefficient serialization but good for now.
func (s *storage) Save(w io.Writer) error {
	s.l.Printf("Save()")
	rr, err := s.GetAll()
	if err != nil {
		return fmt.Errorf("get records: %v", err)
	}
	bytes, err := proto.Marshal(&api.Records{Records: rr})
	if err != nil {
		return fmt.Errorf("save marshal: %v", err)
	}
	_, err = w.Write(bytes)
	if err != nil {
		return fmt.Errorf("save write: %v", err)
	}
	return nil
}

// Load deserializes storage from the given reader.
func (s *storage) Load(r io.Reader) error {
	s.l.Printf("Load()")
	bytes, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("load readall: %v", err)
	}
	var records api.Records
	if err := proto.Unmarshal(bytes, &records); err != nil {
		return fmt.Errorf("load unmarshal: %v", err)
	}
	s.Lock()
	defer s.Unlock()
	s.m = make(map[string]*api.Record)
	for _, r := range records.Records {
		s.m[r.Key] = r
	}
	return nil
}
