package coordination

import (
	"context"
	"time"

	"github.com/storage-lock/go-storage"
)

// Semaphore 是基于 CAS 的计数信号量。
type Semaphore struct {
	ns *Namespace
	id string
}

func (x *Namespace) Semaphore(id string, permits int) (*Semaphore, error) {
	if permits <= 0 {
		return nil, ErrInvalidCount
	}
	semaphore := &Semaphore{ns: x, id: id}
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{Permits: permits, Version: 1})
	})
	if err != nil {
		return nil, err
	}
	return semaphore, nil
}

// TryAcquire 在没有许可时立即返回 ErrNoPermit。
func (x *Semaphore) TryAcquire(ctx context.Context) error {
	return retry(ctx, func() error {
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrNoPermit
			}
			return err
		}
		if current.Permits <= 0 {
			return ErrNoPermit
		}
		next := *current
		next.Permits--
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}

// Release 归还一个许可，并推进 signal 以便等待者被唤醒。
func (x *Semaphore) Release(ctx context.Context) error {
	return retry(ctx, func() error {
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrNoPermit
			}
			return err
		}
		next := *current
		next.Permits++
		next.Signal++
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}

// Acquire 等待直到获得许可或 ctx 结束。
func (x *Semaphore) Acquire(ctx context.Context) error {
	for {
		err := x.TryAcquire(ctx)
		if err == nil || (err != nil && err != ErrNoPermit) {
			return err
		}
		if err := x.ns.waitSignal(ctx, x.id); err != nil {
			return err
		}
	}
}

// Latch 在计数归零前阻塞等待者。
type Latch struct {
	ns *Namespace
	id string
}

func (x *Namespace) Latch(id string, count int) (*Latch, error) {
	if count <= 0 {
		return nil, ErrInvalidCount
	}
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{Count: count, Version: 1})
	})
	if err != nil {
		return nil, err
	}
	return &Latch{ns: x, id: id}, nil
}

// CountDown 将计数减一。已经为零时返回 ErrAlreadyZero。
func (x *Latch) CountDown(ctx context.Context) error {
	return retry(ctx, func() error {
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrAlreadyZero
			}
			return err
		}
		if current.Count <= 0 {
			return ErrAlreadyZero
		}
		next := *current
		next.Count--
		next.Signal++
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}

// Wait 阻塞到计数为零或 ctx 结束。
func (x *Latch) Wait(ctx context.Context) error {
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

// Condition 提供跨实例的 Signal 与 Wait。
type Condition struct {
	ns *Namespace
	id string
}

func (x *Namespace) Condition(id string) (*Condition, error) {
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{Version: 1})
	})
	if err != nil {
		return nil, err
	}
	return &Condition{ns: x, id: id}, nil
}

// Signal 唤醒后续 Wait。没有存储 Pub/Sub 时，等待方通过版本变化感知。
func (x *Condition) Signal(ctx context.Context) error {
	return retry(ctx, func() error {
		current, err := x.ns.load(ctx, x.id)
		if err != nil || current == nil {
			if err == nil {
				err = ErrNoPermit
			}
			return err
		}
		next := *current
		next.Signal++
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}

// Wait 从调用时刻开始等待下一次 Signal。
func (x *Condition) Wait(ctx context.Context) error {
	start, err := x.ns.load(ctx, x.id)
	if err != nil || start == nil {
		if err == nil {
			err = ErrNoPermit
		}
		return err
	}
	seen := start.Signal
	for {
		current, err := x.ns.load(ctx, x.id)
		if err != nil {
			return err
		}
		if current != nil && current.Signal != seen {
			return nil
		}
		if err := x.ns.waitSignal(ctx, x.id); err != nil {
			return err
		}
	}
}

func (x *Namespace) waitSignal(ctx context.Context, id string) error {
	interval := x.wait
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}
	watchable, ok := x.store.(storage.LockWatchable)
	if !ok {
		if adapter, adapted := x.store.(*StorageAdapter); adapted {
			watchable, ok = adapter.storage.(storage.LockWatchable)
			id = x.key(id)
		}
	}
	var updates <-chan *storage.LockInformation
	if ok {
		ch, err := watchable.Watch(ctx, id)
		if err == nil {
			updates = ch
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	if updates == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-updates:
		return nil
	case <-timer.C:
		return nil
	}
}
