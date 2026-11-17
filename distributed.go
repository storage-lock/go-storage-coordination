package coordination

import (
	"context"
	"sort"
	"time"
)

// Lock 是组合锁需要的最小互斥接口，避免协调模块反向依赖具体实现。
type Lock interface {
	Lock(ctx context.Context, ownerId string) error
	UnLock(ctx context.Context, ownerId string) error
}

// MultiLock 按固定顺序获取多把锁，避免调用方各自乱序加锁造成死锁。
// 它复用已有 StorageLock，不改变单锁互斥、租约和 fencing 语义。
type MultiLock struct {
	locks []Lock
}

// NewMultiLock 复制并按锁 ID 排序。空集合或空 ID 都拒绝。
func NewMultiLock(locks map[string]Lock) (*MultiLock, error) {
	if len(locks) == 0 {
		return nil, ErrInvalidCount
	}
	ids := make([]string, 0, len(locks))
	for id, lock := range locks {
		if id == "" || lock == nil {
			return nil, ErrInvalidCount
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordered := make([]Lock, 0, len(ids))
	for _, id := range ids {
		ordered = append(ordered, locks[id])
	}
	return &MultiLock{locks: ordered}, nil
}

// Lock 成功时全部持有。中途失败会逆序释放已经获得的锁。
func (x *MultiLock) Lock(ctx context.Context, ownerId string) error {
	for index, lock := range x.locks {
		if err := lock.Lock(ctx, ownerId); err != nil {
			x.rollback(ownerId, index)
			return err
		}
	}
	return nil
}

// Unlock 逆序释放，返回遇到的第一个错误，但继续释放剩余锁。
func (x *MultiLock) Unlock(ctx context.Context, ownerId string) error {
	var first error
	for index := len(x.locks) - 1; index >= 0; index-- {
		if err := x.locks[index].UnLock(ctx, ownerId); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (x *MultiLock) rollback(ownerId string, acquired int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for index := acquired - 1; index >= 0; index-- {
		_ = x.locks[index].UnLock(ctx, ownerId)
	}
}

// Barrier 让固定数量的参与者同时通过。
type Barrier struct {
	ns      *Namespace
	id      string
	parties int
}

// Barrier 创建或复用一个栅栏。parties 必须为正数。
func (x *Namespace) Barrier(id string, parties int) (*Barrier, error) {
	if parties <= 0 {
		return nil, ErrInvalidCount
	}
	barrier := &Barrier{ns: x, id: id, parties: parties}
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{Count: parties, Version: 1})
	})
	return barrier, err
}

// Arrive 登记一次到达。最后一个参与者打开栅栏，其余参与者等待打开。
func (x *Barrier) Arrive(ctx context.Context) error {
	type arrival struct{ opened bool }
	result := &arrival{}
	err := retry(ctx, func() error {
		result.opened = false
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrBrokenBarrier
			}
			return err
		}
		if current.Count <= 0 {
			result.opened = true
			return nil
		}
		next := *current
		next.Count--
		if next.Count == 0 {
			next.Signal++
			result.opened = true
		}
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
	if err != nil || result.opened {
		return err
	}
	return x.wait(ctx)
}

func (x *Barrier) wait(ctx context.Context) error {
	for {
		current, err := x.ns.load(ctx, x.id)
		if err != nil {
			return err
		}
		if current != nil && current.Count <= 0 {
			return nil
		}
		if err := x.ns.waitSignal(ctx, x.id); err != nil {
			return err
		}
	}
}

// RateLimiter 是跨实例共享的固定窗口限流器。
// 窗口以调用方传入的单调时钟推进，测试可注入虚拟时间。
type RateLimiter struct {
	ns     *Namespace
	id     string
	limit  int
	window time.Duration
	now    func() time.Time
}

// TimeProvider 是协调原语使用的统一时间源。
type TimeProvider interface {
	GetTime(ctx context.Context) (time.Time, error)
}

func (x *RateLimiter) current(ctx context.Context) int64 {
	if x.ns.clock != nil {
		if now, err := x.ns.clock.GetTime(ctx); err == nil {
			return now.UnixNano()
		}
	}
	return x.now().UnixNano()
}

// RateLimiter 创建每 window 最多 limit 次的限流器。
func (x *Namespace) RateLimiter(id string, limit int, window time.Duration) (*RateLimiter, error) {
	if limit <= 0 || window <= 0 {
		return nil, ErrInvalidCount
	}
	limiter := &RateLimiter{ns: x, id: id, limit: limit, window: window, now: time.Now}
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{Permits: limit, Version: 1})
	})
	return limiter, err
}

// Allow 在当前窗口内消耗一个额度。超额返回 ErrRateLimited。
func (x *RateLimiter) Allow(ctx context.Context) error {
	now := x.current(ctx)
	return retry(ctx, func() error {
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrRateLimited
			}
			return err
		}
		next := *current
		if current.WindowStart == 0 || now-current.WindowStart >= int64(x.window) {
			next.Permits = x.limit
			next.WindowStart = now
		}
		if next.Permits <= 0 {
			return ErrRateLimited
		}
		next.Permits--
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}
