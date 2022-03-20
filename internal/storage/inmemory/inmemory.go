package inmemory

import (
	"fmt"
	"io"
	"log"
	"sync"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"google.golang.org/protobuf/proto"
)

type RecordNotFound struct {
	key string
}

func (rnf RecordNotFound) Error() string {
	return fmt.Sprintf("record with key %q not found", rnf.key)
}

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
	s.m[r.Key] = r
	return nil
}

// Get returns record associated with the given key.
// If record not found, RecordNotFound error is returned.
func (s *storage) Get(key string) (*api.Record, error) {
	s.RLock()
	defer s.RUnlock()
	r, ok := s.m[key]
	if !ok {
		return nil, RecordNotFound{key: key}
	}
	return r, nil
}

// Delete deletes record associated with the given key.
func (s *storage) Delete(key string) error {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
	return nil
}

// Reset resets the storage.
func (s *storage) Reset() error {
	s.Lock()
	defer s.Unlock()
	s.m = make(map[string]*api.Record)
	return nil
}

// Save serializes storage into the given writer.
// Inefficient serialization but good for now.
func (s *storage) Save(w io.Writer) error {
	var rr []*api.Record
	s.RLock()
	for _, r := range s.m {
		rr = append(rr, r)
	}
	s.RUnlock()
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
