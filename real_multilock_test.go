package coordination_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	coordination "github.com/storage-lock/go-storage-coordination"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

func TestMultiLockRealStorageMutualExclusion(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	left, err := storage_lock.NewStorageLock(store, "resource-a")
	if err != nil {
		t.Fatal(err)
	}
	right, err := storage_lock.NewStorageLock(store, "resource-b")
	if err != nil {
		t.Fatal(err)
	}
	first, err := coordination.NewMultiLock(map[string]coordination.Lock{"b": right, "a": left})
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordination.NewMultiLock(map[string]coordination.Lock{"a": left, "b": right})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var overlap atomic.Int32
	var current atomic.Int32
	var wg sync.WaitGroup
	run := func(multi *coordination.MultiLock, owner string) {
		defer wg.Done()
		if err := multi.Lock(ctx, owner); err != nil {
			t.Error(err)
			return
		}
		seen := current.Add(1)
		if seen > 1 {
			overlap.Add(1)
		}
		time.Sleep(30 * time.Millisecond)
		current.Add(-1)
		if err := multi.Unlock(ctx, owner); err != nil {
			t.Error(err)
		}
	}
	wg.Add(2)
	go run(first, "owner-1")
	go run(second, "owner-2")
	wg.Wait()
	if overlap.Load() != 0 {
		t.Fatalf("critical sections overlapped %d times", overlap.Load())
	}
}
