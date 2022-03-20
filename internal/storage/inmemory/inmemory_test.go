package inmemory

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	api "github.com/alkur-gh/kmpv2/api/v1"
)

// TestPutGetDelete tests one record workflow.
func TestPutGetDelete(t *testing.T) {
	var err error
	s := newTestStorage(t)
	want := &api.Record{Key: "key 1", Value: []byte("value 1")}
	putGet(t, s, want)
	if err := s.Delete(want.Key); err != nil {
		t.Errorf("Delete(%v) error: %v", want.Key, err)
	}
	_, err = s.Get(want.Key)
	if _, ok := err.(RecordNotFound); !ok {
		t.Errorf("Get(%v) expected RecordNotFound error; but got: %v", want.Key, err)
	}
}

// TestConcurrentDisjointRecords tests concurrent puts and gets.
func TestConcurrentDisjointRecords(t *testing.T) {
	const N = 1000
	s := newTestStorage(t)
	wg := &sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(id int) {
			defer wg.Done()
			putGet(t, s, &api.Record{
				Key:   fmt.Sprintf("Key %d", id),
				Value: []byte(fmt.Sprintf("Value %d", id)),
			})
		}(i)
	}
	wg.Wait()
}

// TestReset tests storage resets.
func TestReset(t *testing.T) {
	s := newTestStorage(t)
	r := &api.Record{Key: "Key 1", Value: []byte("Value 1")}
	putGet(t, s, r)
	if err := s.Reset(); err != nil {
		t.Errorf("Reset() unexpected error: %v", err)
	}
	_, err := s.Get(r.Key)
	if _, ok := err.(RecordNotFound); !ok {
		t.Errorf("Get(%v) expected RecordNotFound error; but got: %v", r.Key, err)
	}
}

// TestSaveLoad tests saving and loading records in storage.
func TestSaveLoad(t *testing.T) {
	var err error
	s := newTestStorage(t)
	rr := []api.Record{
		{
			Key:   "key 1",
			Value: []byte("value 1"),
		},
		{
			Key:   "key 2",
			Value: []byte("value 2"),
		},
	}
	// not using range bcs go vet warns about lock copy
	// not sure how im supposed to deal with this
	for i := 0; i < len(rr); i++ {
		r := api.Record{Key: rr[i].Key, Value: rr[i].Value} // required
		putGet(t, s, &r)
	}
	f, err := os.CreateTemp("", "TestSave-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	s.Save(f)
	if err := f.Sync(); err != nil {
		t.Errorf("Sync() failed: %v", err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Errorf("Stat() failed: %v", err)
	}
	if fi.Size() == 0 {
		t.Errorf("save file is empty")
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close() failed: %v", err)
		}
	}()
	s.Reset()
	if _, err := f.Seek(0, 0); err != nil {
		t.Errorf("Seek(0, 0) unexpected error: %v", err)
	}
	if err := s.Load(f); err != nil {
		t.Errorf("Load() unexpected error: %v", err)
	}
	for i := 0; i < len(rr); i++ {
		want := api.Record{Key: rr[i].Key, Value: rr[i].Value}
		got, err := s.Get(want.Key)
		if err != nil {
			t.Fatalf("Get(%v) unexpected error: %v", want.Key, err)
		}
		if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Get(%v) = %v; want %v", want.Key, got, &want)
		}
	}
}

func newTestStorage(t *testing.T) *storage {
	t.Helper()
	s, err := New(nil)
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
	return s
}

func putGet(t *testing.T, s *storage, want *api.Record) {
	t.Helper()
	if err := s.Put(want); err != nil {
		t.Errorf("Put(%v) unexpected error: %v", want, err)
	}
	got, err := s.Get(want.Key)
	if err != nil {
		t.Errorf("Get(%v) error: %v", want.Key, err)
	}
	if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) {
		t.Errorf("Get(%v) = %v; want %v", want.Key, got, want)
	}
}
