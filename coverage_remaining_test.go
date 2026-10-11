package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

type scriptStore struct {
	*MemoryStore
	getErr    error
	failAfter int
	gets      int
	raw       map[string]string
}

func (x *scriptStore) Get(ctx context.Context, id string) (string, error) {
	x.gets++
	if x.failAfter > 0 && x.gets > x.failAfter {
		return "", x.getErr
	}
	if x.getErr != nil && x.failAfter == 0 {
		return "", x.getErr
	}
	if value, ok := x.raw[id]; ok {
		return value, nil
	}
	return x.MemoryStore.Get(ctx, id)
}

type branchLock struct {
	lockErr, unlockErr error
	locked             int
}

func (x *branchLock) Lock(context.Context, string) error {
	if x.lockErr != nil {
		return x.lockErr
	}
	x.locked++
	return nil
}
func (x *branchLock) UnLock(context.Context, string) error {
	if x.unlockErr != nil {
		return x.unlockErr
	}
	if x.locked > 0 {
		x.locked--
	}
	return nil
}

func TestCoordinationRemainingBranches(t *testing.T) {
	ctx := context.Background()
	if _, err := NewNamespace("x", nil); err == nil {
		t.Fatal("nil store accepted")
	}
	ns, err := NewNamespace("tenant", NewMemoryStore())
	if err != nil || ns.Name() != "tenant" {
		t.Fatal(err)
	}
	if _, err := ns.Semaphore("bad", 0); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	if _, err := ns.Latch("bad", 0); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	if _, err := ns.Barrier("bad", 0); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	if _, err := ns.RateLimiter("bad", 0, time.Second); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}

	store := &scriptStore{MemoryStore: NewMemoryStore(), getErr: errors.New("read failed")}
	failing, err := NewNamespace("failed", store)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []func() error{
		func() error { _, err := failing.Semaphore("s", 1); return err },
		func() error { _, err := failing.Latch("l", 1); return err },
		func() error { _, err := failing.Barrier("b", 1); return err },
		func() error { _, err := failing.RateLimiter("r", 1, time.Second); return err },
		func() error { _, err := failing.Condition("c"); return err },
	} {
		if err := op(); !errors.Is(err, store.getErr) {
			t.Fatalf("create err=%v", err)
		}
	}
	store.getErr = nil
	store.raw = map[string]string{failing.key("broken"): "{"}
	if _, err := failing.observe(ctx, "broken"); err == nil {
		t.Fatal("invalid json accepted")
	}
	store.raw[failing.key("empty")] = ""
	if value, err := failing.observe(ctx, "empty"); err != nil || value != nil {
		t.Fatalf("empty value=%v err=%v", value, err)
	}
	if _, err := failing.Semaphore("broken", 1); err == nil {
		t.Fatal("existing invalid record accepted")
	}

	semaphore, err := ns.Semaphore("jobs", 1)
	if err != nil {
		t.Fatal(err)
	}
	missing := &Semaphore{ns: ns, id: "missing"}
	if err := missing.TryAcquire(ctx); !errors.Is(err, ErrNoPermit) {
		t.Fatal(err)
	}
	if err := missing.Release(ctx); !errors.Is(err, ErrNoPermit) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := missing.Acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := semaphore.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}
	store.getErr = errors.New("runtime read")
	runtime, _ := NewNamespace("runtime", store)
	brokenSemaphore := &Semaphore{ns: runtime, id: "jobs"}
	if err := brokenSemaphore.TryAcquire(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	if err := brokenSemaphore.Release(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	store.getErr = nil

	latch := &Latch{ns: ns, id: "missing-latch"}
	if err := latch.CountDown(ctx); !errors.Is(err, ErrAlreadyZero) {
		t.Fatal(err)
	}
	if err := latch.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	realLatch, err := ns.Latch("batch", 1)
	if err != nil || realLatch.CountDown(ctx) != nil || realLatch.CountDown(ctx) == nil {
		t.Fatal("latch zero branch")
	}
	if err := realLatch.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	brokenLatch := &Latch{ns: runtime, id: "batch"}
	store.getErr = errors.New("latch read")
	if err := brokenLatch.CountDown(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	if err := brokenLatch.Wait(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	store.getErr = nil

	condition := &Condition{ns: ns, id: "missing-condition"}
	if err := condition.Signal(ctx); !errors.Is(err, ErrNoPermit) {
		t.Fatal(err)
	}
	if err := condition.Wait(ctx); !errors.Is(err, ErrNoPermit) {
		t.Fatal(err)
	}
	realCondition, err := ns.Condition("ready")
	if err != nil || realCondition.Signal(ctx) != nil {
		t.Fatal(err)
	}
	store.getErr = errors.New("condition read")
	brokenCondition := &Condition{ns: runtime, id: "ready"}
	if err := brokenCondition.Signal(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	if err := brokenCondition.Wait(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	store.getErr = nil
	short, cancelShort := context.WithTimeout(ctx, time.Millisecond)
	defer cancelShort()
	if err := realCondition.Wait(short); err == nil {
		t.Fatal("condition ignored deadline")
	}

	barrier, err := ns.Barrier("gate", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Barrier{ns: ns, id: "missing-barrier"}).Arrive(ctx); !errors.Is(err, ErrBrokenBarrier) {
		t.Fatal(err)
	}
	waiting, waitCancel := context.WithCancel(ctx)
	arrived := make(chan error, 1)
	go func() { arrived <- barrier.Arrive(waiting) }()
	time.Sleep(20 * time.Millisecond)
	waitCancel()
	select {
	case err := <-arrived:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled arrival=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("barrier wait did not observe cancellation")
	}
	if err := barrier.Arrive(ctx); err != nil {
		t.Fatalf("opening arrival: %v", err)
	}
	again := make(chan error, 1)
	go func() { again <- barrier.Arrive(ctx) }()
	select {
	case err := <-again:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("open barrier did not release waiter")
	}
	store.getErr = errors.New("barrier read")
	if err := (&Barrier{ns: runtime, id: "gate"}).Arrive(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	store.getErr = nil

	limiter, err := ns.RateLimiter("api", 1, time.Hour)
	if err != nil || limiter.Allow(ctx) != nil {
		t.Fatal(err)
	}
	if err := limiter.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
	limiter.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if err := limiter.Allow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := (&RateLimiter{ns: ns, id: "missing-rate", limit: 1, window: time.Second, now: time.Now}).Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
	store.getErr = errors.New("rate read")
	if err := (&RateLimiter{ns: runtime, id: "api", limit: 1, window: time.Second, now: time.Now}).Allow(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}

	if _, err := NewMultiLock(nil); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	if _, err := NewMultiLock(map[string]Lock{"": &branchLock{}}); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	if _, err := NewMultiLock(map[string]Lock{"a": nil}); !errors.Is(err, ErrInvalidCount) {
		t.Fatal(err)
	}
	lockErr := errors.New("lock failed")
	unlockErr := errors.New("unlock failed")
	first, second := &branchLock{unlockErr: unlockErr}, &branchLock{lockErr: lockErr}
	multi, err := NewMultiLock(map[string]Lock{"b": second, "a": first})
	if err != nil {
		t.Fatal(err)
	}
	if err := multi.Lock(ctx, "owner"); !errors.Is(err, lockErr) || first.locked != 1 {
		t.Fatalf("rollback lock=%v held=%d", err, first.locked)
	}
	first.unlockErr = nil
	second = &branchLock{lockErr: lockErr}
	multi, err = NewMultiLock(map[string]Lock{"b": second, "a": &branchLock{}})
	if err != nil {
		t.Fatal(err)
	}
	clean := multi.locks[0].(*branchLock)
	if err := multi.Lock(ctx, "owner"); !errors.Is(err, lockErr) || clean.locked != 0 {
		t.Fatalf("successful rollback lock=%v held=%d", err, clean.locked)
	}
	first.lockErr, second.lockErr = nil, nil
	multi, err = NewMultiLock(map[string]Lock{"b": second, "a": first})
	if err != nil {
		t.Fatal(err)
	}
	second.unlockErr = unlockErr
	if err := multi.Lock(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := multi.Unlock(ctx, "owner"); !errors.Is(err, unlockErr) {
		t.Fatal(err)
	}

	versionStore := &versionMissStore{MemoryStore: NewMemoryStore()}
	versionNS, err := NewNamespace("versions", versionStore)
	if _, err = versionNS.Semaphore("once", 1); err != nil {
		t.Fatal(err)
	}

	var panicked bool
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		_ = NewStorageAdapter(nil).Create(ctx, "record", 1, "owner")
	}()
	if !panicked {
		t.Fatal("nil storage adapter did not panic")
	}
	adapter := NewStorageAdapter(memory_storage.NewMemoryStorage())
	if err := adapter.Create(ctx, "record", 1, "owner"); err != nil {
		t.Fatal(err)
	}
	raw, err := adapter.Get(ctx, "record")
	if err != nil || raw == "" {
		t.Fatalf("adapter get raw=%q err=%v", raw, err)
	}
	if err := adapter.Update(ctx, "record", 1, 2, "next"); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Get(ctx, "missing"); !errors.Is(err, storage_lock.ErrLockNotFound) {
		t.Fatal(err)
	}
	if value, err := versionNS.observe(ctx, "once"); err != nil || value == nil || value.Permits != 1 {
		t.Fatalf("version miss state=%v err=%v", value, err)
	}

	dup := NewMemoryStore()
	if err := dup.Create(ctx, "same", 1, "{}"); err != nil || !errors.Is(dup.Create(ctx, "same", 2, "{}"), storage_lock.ErrVersionMiss) {
		t.Fatal("duplicate create")
	}

	permit, err := ns.Semaphore("wait-permit", 1)
	if err != nil || permit.TryAcquire(ctx) != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() { acquired <- permit.Acquire(ctx) }()
	time.Sleep(15 * time.Millisecond)
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("semaphore acquire did not resume")
	}
	hard := errors.New("acquire failed")
	store.getErr = hard
	if err := (&Semaphore{ns: runtime, id: "jobs"}).Acquire(ctx); !errors.Is(err, hard) {
		t.Fatal(err)
	}
	store.getErr = nil
	missed, missCancel := context.WithCancel(ctx)
	missCancel()
	if err := retry(missed, func() error { return storage_lock.ErrVersionMiss }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	readBarrier := &Barrier{ns: runtime, id: "gate"}
	store.getErr = errors.New("barrier wait read")
	if err := readBarrier.wait(ctx); !errors.Is(err, store.getErr) {
		t.Fatal(err)
	}
	store.getErr = nil
	live, err := ns.Condition("live")
	if err != nil || live.Signal(ctx) != nil {
		t.Fatal(err)
	}
	signaled := make(chan error, 1)
	go func() { signaled <- live.Wait(ctx) }()
	time.Sleep(15 * time.Millisecond)
	if err := live.Signal(ctx); err != nil || live.Signal(ctx) != nil {
		t.Fatal(err)
	}
	select {
	case err := <-signaled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("condition did not observe signal")
	}
	changed := errors.New("condition changed")
	store.getErr = changed
	if err := (&Condition{ns: runtime, id: "ready"}).Wait(ctx); !errors.Is(err, changed) {
		t.Fatal(err)
	}
	store.getErr = nil
	loopStore := &scriptStore{MemoryStore: NewMemoryStore(), getErr: errors.New("loop read"), failAfter: 3}
	loopNS, err := NewNamespace("loop", loopStore)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loopNS.Condition("flip"); err != nil {
		t.Fatal(err)
	}
	if err := (&Condition{ns: loopNS, id: "flip"}).Wait(ctx); !errors.Is(err, loopStore.getErr) {
		t.Fatal(err)
	}
	memory := NewMemoryStore()
	if err := memory.Create(ctx, "item", 1, "{}"); err != nil {
		t.Fatal(err)
	}
	if err := memory.Update(ctx, "missing", 1, 2, "{}"); !errors.Is(err, storage_lock.ErrVersionMiss) {
		t.Fatal(err)
	}
	if err := memory.Update(ctx, "item", 9, 2, "{}"); !errors.Is(err, storage_lock.ErrVersionMiss) {
		t.Fatal(err)
	}
}

type versionMissStore struct{ *MemoryStore }

func (x *versionMissStore) Create(ctx context.Context, id string, version uint64, value string) error {
	if err := x.MemoryStore.Create(ctx, id, version, value); err != nil {
		return err
	}
	return storage_lock.ErrVersionMiss
}
