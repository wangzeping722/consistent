package consistent

import "sync"

const (
	TopWeight = 100

	minReplica = 100
	prime      = 167777619
)

type (
	PlaceholderType = struct{}

	HashFunc func(data []byte) uint64

	ConsistentHash struct {
		hashFunc HashFunc
		replicas int
		keys     []uint64
		ring     map[uint64][]interface{}
		nodes    map[string]PlaceholderType
		lock     sync.RWMutex
	}
)

// NewCustomConsistentHash returns a ConsistentHash with given replicas and hash func
func NewCustomConsistentHash(replicas int, fn HashFunc) *ConsistentHash {
	if replicas < minReplica {
		replicas = minReplica
	}

	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]interface{}),
		nodes:    make(map[string]PlaceholderType),
	}
}
