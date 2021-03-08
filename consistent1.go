package consistent

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

const (
	TopWeight = 100

	minReplica = 100
	prime      = 167777619
)

var Placeholder PlaceholderType

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

// NewConsistentHash returns a default ConsistentHash
func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplica, Hash)
}

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

func (h *ConsistentHash) Add(node interface{}) {
	h.AddWithReplicas(node, h.replicas)
}

func (h *ConsistentHash) AddWithReplicas(node interface{}, replicas int) {
	h.Remove(node)

	if replicas > h.replicas {
		replicas = h.replicas
	}

	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()
	h.addNode(nodeRepr)

	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *ConsistentHash) AddWithWeight(node interface{}, weight int) {
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

func (h *ConsistentHash) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = Placeholder
}

func (h *ConsistentHash) Remove(node interface{}) {
	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.containsNode(nodeRepr) {
		return
	}

	for i := 0; i < h.replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		if index < len(h.keys) {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, nodeRepr)
	}
}

// removeRingNode 移除虚拟节点
func (h *ConsistentHash) removeRingNode(hash uint64, nodeRepr string) {
	if nodes, ok := h.ring[hash]; ok {
		newNodes := nodes[:0]
		for _, x := range nodes {
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}
		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *ConsistentHash) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

func (h *ConsistentHash) Get(v interface{}) (interface{}, bool) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if len(h.ring) == 0 {
		return nil, false
	}

	hash := h.hashFunc([]byte(repr(v)))
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)
	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

func (h *ConsistentHash) containsNode(nodeRepr string) bool {
	_, ok := h.nodes[nodeRepr]
	return ok
}

func repr(node interface{}) string {
	if node == nil {
		return ""
	}

	switch nt := node.(type) {
	case fmt.Stringer:
		return nt.String()
	}

	val := reflect.ValueOf(node)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}
	return reprOfValue(val)
}

func reprOfValue(val reflect.Value) string {
	switch vt := val.Interface().(type) {
	case bool:
		return strconv.FormatBool(vt)
	case error:
		return vt.Error()
	case float32:
		return strconv.FormatFloat(float64(vt), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(vt, 'f', -1, 64)
	case fmt.Stringer:
		return vt.String()
	case int:
		return strconv.Itoa(vt)
	case int8:
		return strconv.Itoa(int(vt))
	case int16:
		return strconv.Itoa(int(vt))
	case int32:
		return strconv.Itoa(int(vt))
	case int64:
		return strconv.FormatInt(vt, 10)
	case string:
		return vt
	case uint:
		return strconv.FormatUint(uint64(vt), 10)
	case uint8:
		return strconv.FormatUint(uint64(vt), 10)
	case uint16:
		return strconv.FormatUint(uint64(vt), 10)
	case uint32:
		return strconv.FormatUint(uint64(vt), 10)
	case uint64:
		return strconv.FormatUint(uint64(vt), 10)
	case []byte:
		return string(vt)
	default:
		return fmt.Sprint(val.Interface())
	}
}

func innerRepr(node interface{}) string {
	return fmt.Sprintf("%d:%v", prime, node)
}