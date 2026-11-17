package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	ErrNoPermit       = errors.New("semaphore has no permit")
	ErrAlreadyZero    = errors.New("latch is already zero")
	ErrInvalidCount   = errors.New("coordination count must be positive")
	ErrNamespaceEmpty = errors.New("namespace can not be empty")
	ErrBrokenBarrier  = errors.New("barrier is broken")
	ErrRateLimited    = errors.New("rate limit exceeded")
)

// Store 是协调原语需要的最小原子存储。
type Store interface {
	Get(ctx context.Context, id string) (string, error)
	Create(ctx context.Context, id string, version uint64, value string) error
	Update(ctx context.Context, id string, expectedVersion, newVersion uint64, value string) error
}

// Namespace 给同一存储中的不同租户隔离记录键。
type Namespace struct {
	name  string
	store Store
	clock TimeProvider
	// wait 是没有存储通知时的兜底轮询间隔。
	wait time.Duration
}

// SetTimeProvider 设置命名空间内限流器使用的统一时间源。
// 不设置时使用各限流器自己的本地时钟。
func (x *Namespace) SetTimeProvider(clock TimeProvider) { x.clock = clock }

func NewNamespace(name string, store Store) (*Namespace, error) {
	if name == "" {
		return nil, ErrNamespaceEmpty
	}
	if store == nil {
		return nil, errors.New("store can not be nil")
	}
	return &Namespace{name: name, store: store, wait: 5 * time.Millisecond}, nil
}

// SetWaitInterval 设置没有存储通知时的轮询间隔。
// 小于等于 0 时保持默认 5ms。存储实现了 Watch 时，等待会被通知提前唤醒。
func (x *Namespace) SetWaitInterval(interval time.Duration) {
	if interval <= 0 {
		return
	}
	x.wait = interval
}

func (x *Namespace) key(id string) string { return x.name + "\x00" + id }
func (x *Namespace) Name() string         { return x.name }

type state struct {
	Permits int    `json:"permits"`
	Count   int    `json:"count"`
	Signal  uint64 `json:"signal"`
	Version uint64 `json:"version"`
	// WindowStart 是当前限流窗口起点，单位纳秒，取自统一时间源。
	WindowStart int64 `json:"window_start"`
	// TokensMilli 是令牌桶剩余令牌，以毫令牌计。
	TokensMilli int64 `json:"tokens_milli"`
	// UpdatedAt 是令牌桶上次补充的时间，单位纳秒。
	UpdatedAt int64 `json:"updated_at"`
}

func (x *Namespace) load(ctx context.Context, id string) (*state, error) {
	raw, err := x.store.Get(ctx, x.key(id))
	if err != nil {
		if errors.Is(err, storage_lock.ErrLockNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var value state
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func (x *Namespace) swap(ctx context.Context, id string, current, next *state) error {
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if current == nil {
		return x.store.Create(ctx, x.key(id), next.Version, string(payload))
	}
	return x.store.Update(ctx, x.key(id), current.Version, next.Version, string(payload))
}

func retry(ctx context.Context, operation func() error) error {
	for {
		err := operation()
		if !errors.Is(err, storage_lock.ErrVersionMiss) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// MemoryStore 是进程内 CAS 存储，测试和单进程协调可以直接使用。
type MemoryStore struct {
	mu    sync.Mutex
	items map[string]memoryItem
}

type memoryItem struct {
	version uint64
	value   string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: map[string]memoryItem{}}
}

func (x *MemoryStore) Get(_ context.Context, id string) (string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	item, ok := x.items[id]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return item.value, nil
}

func (x *MemoryStore) Create(_ context.Context, id string, version uint64, value string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, ok := x.items[id]; ok {
		return storage_lock.ErrVersionMiss
	}
	x.items[id] = memoryItem{version: version, value: value}
	return nil
}

func (x *MemoryStore) Update(_ context.Context, id string, expectedVersion, newVersion uint64, value string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	item, ok := x.items[id]
	if !ok || item.version != expectedVersion {
		return storage_lock.ErrVersionMiss
	}
	x.items[id] = memoryItem{version: newVersion, value: value}
	return nil
}

// StorageAdapter 把现有 storage.Storage 适配为协调原语存储。
type StorageAdapter struct{ storage storage.Storage }

func NewStorageAdapter(value storage.Storage) *StorageAdapter { return &StorageAdapter{storage: value} }

func (x *StorageAdapter) Get(ctx context.Context, id string) (string, error) {
	return x.storage.Get(ctx, id)
}

func (x *StorageAdapter) Create(ctx context.Context, id string, version uint64, value string) error {
	return x.storage.CreateWithVersion(ctx, id, storage.Version(version), information(id, version, value))
}

func (x *StorageAdapter) Update(ctx context.Context, id string, expectedVersion, newVersion uint64, value string) error {
	return x.storage.UpdateWithVersion(ctx, id, storage.Version(expectedVersion), storage.Version(newVersion), information(id, newVersion, value))
}

func information(id string, version uint64, value string) *storage.LockInformation {
	return &storage.LockInformation{LockId: id, OwnerId: value, Version: storage.Version(version), LockCount: 1}
}

// observe 读取当前状态。记录不存在时返回 nil，不视为错误。
func (x *Namespace) observe(ctx context.Context, id string) (*state, error) {
	return x.load(ctx, id)
}
