package coordination

import (
	"context"
	"sync"
	"testing"
	"time"

	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/require"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) GetTime(context.Context) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now, nil
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type memoryLock struct {
	mu    sync.Mutex
	owner string
	depth int
}

func (l *memoryLock) Lock(_ context.Context, owner string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner != "" && l.owner != owner {
		return storage_lock.ErrLockBusy
	}
	l.owner = owner
	l.depth++
	return nil
}

func (l *memoryLock) UnLock(_ context.Context, owner string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner != owner || l.depth == 0 {
		return storage_lock.ErrLockNotBelongYou
	}
	l.depth--
	if l.depth == 0 {
		l.owner = ""
	}
	return nil
}

func (l *memoryLock) heldBy(owner string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.owner == owner && l.depth > 0
}

func TestLockConditionReacquiresAfterSignal(t *testing.T) {
	ns, err := NewNamespace("orders", NewMemoryStore())
	require.NoError(t, err)
	lock := &memoryLock{}
	require.NoError(t, lock.Lock(context.Background(), "worker"))
	condition, err := ns.LockCondition("ready", lock, "worker")
	require.NoError(t, err)

	released := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- condition.Wait(context.Background())
	}()
	require.Eventually(t, func() bool {
		if lock.heldBy("worker") {
			return false
		}
		select {
		case <-released:
		default:
			close(released)
		}
		return true
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, condition.Signal(context.Background()))
	require.NoError(t, <-done)
	require.True(t, lock.heldBy("worker"), "被唤醒后必须重新持有同一把锁")
	require.NoError(t, lock.UnLock(context.Background(), "worker"))
}

func TestLockConditionRelocksWhenCanceled(t *testing.T) {
	ns, err := NewNamespace("orders", NewMemoryStore())
	require.NoError(t, err)
	lock := &memoryLock{}
	require.NoError(t, lock.Lock(context.Background(), "worker"))
	condition, err := ns.LockCondition("canceled", lock, "worker")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- condition.Wait(ctx) }()
	require.Eventually(t, func() bool { return !lock.heldBy("worker") }, time.Second, 5*time.Millisecond)
	cancel()
	err = <-done
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, lock.heldBy("worker"), "取消后也要重新拿到锁，调用方才能继续在锁内收尾")
}

func TestTokenBucketUsesSharedClock(t *testing.T) {
	ns, err := NewNamespace("traffic", NewMemoryStore())
	require.NoError(t, err)
	clock := &manualClock{now: time.Unix(0, 0)}
	ns.SetTimeProvider(clock)
	bucket, err := ns.TokenBucket("api", 2, time.Second)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, bucket.Allow(ctx))
	require.NoError(t, bucket.Allow(ctx))
	require.ErrorIs(t, bucket.Allow(ctx), ErrRateLimited)

	// 本地时间再怎么走，额度也只认统一时间源。
	bucket.now = func() time.Time { return time.Unix(0, int64(time.Hour)) }
	require.ErrorIs(t, bucket.Allow(ctx), ErrRateLimited)

	clock.Advance(2 * time.Second)
	require.NoError(t, bucket.AllowN(ctx, 2))
	require.ErrorIs(t, bucket.Allow(ctx), ErrRateLimited)
	require.ErrorIs(t, bucket.AllowN(ctx, 3), ErrRateLimited)
}

func TestFixedWindowUsesSharedClock(t *testing.T) {
	ns, err := NewNamespace("traffic", NewMemoryStore())
	require.NoError(t, err)
	clock := &manualClock{now: time.Unix(1_000, 0)}
	ns.SetTimeProvider(clock)
	limiter, err := ns.RateLimiter("create", 1, time.Second)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, limiter.Allow(ctx))
	require.ErrorIs(t, limiter.Allow(ctx), ErrRateLimited)
	limiter.now = func() time.Time { return clock.now.Add(time.Hour) }
	require.ErrorIs(t, limiter.Allow(ctx), ErrRateLimited, "本地时钟不能提前打开新窗口")
	clock.Advance(time.Second)
	require.NoError(t, limiter.Allow(ctx))
}
