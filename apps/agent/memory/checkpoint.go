package memory

import (
	"context"
	"sync"

	"github.com/cloudwego/eino/compose"
)

type inMemoryStore struct {
	mu  sync.RWMutex
	mem map[string][]byte
}

func NewCheckPointStore() compose.CheckPointStore {
	return &inMemoryStore{
		mem: make(map[string][]byte),
	}
}

func (s *inMemoryStore) Set(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[key] = value
	return nil
}

func (s *inMemoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.mem[key]
	return v, ok, nil
}
