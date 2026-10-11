package coordination

import (
	"context"
	"errors"
	"time"

	storage_lock "github.com/storage-lock/go-storage-lock"
)

// ErrConditionLockRequired 表示调用方没有持有条件变量绑定的锁。
var ErrConditionLockRequired = errors.New("condition wait requires the bound lock")

// LockCondition 是绑定到一把独占锁上的条件变量。
// Wait 原子地释放锁、等待下一次 Signal、再重新获取同一把锁。
// 被唤醒不代表条件成立，调用方必须在持锁状态下重新检查条件。
type LockCondition struct {
	inner  *Condition
	lock   Lock
	owner  string
	signal chan struct{}
}

// LockCondition 创建与 lock 绑定的条件变量。owner 必须是后续持有 lock 的那个标识。
func (x *Namespace) LockCondition(id string, lock Lock, owner string) (*LockCondition, error) {
	if lock == nil {
		return nil, errors.New("condition lock can not be nil")
	}
	if owner == "" {
		return nil, storage_lock.ErrOwnerIdEmpty
	}
	inner, err := x.Condition(id)
	if err != nil {
		return nil, err
	}
	return &LockCondition{inner: inner, lock: lock, owner: owner, signal: make(chan struct{}, 1)}, nil
}

// Signal 唤醒一个正在 Wait 的持有者。可以在不持有锁时调用。
func (x *LockCondition) Signal(ctx context.Context) error {
	if err := x.inner.Signal(ctx); err != nil {
		return err
	}
	select {
	case x.signal <- struct{}{}:
	default:
	}
	return nil
}

// Broadcast 唤醒当前命名空间里的等待者。存储侧只有一个 signal 计数，
// 每个等待者观察到计数变化后都会重新抢锁，因此一次 Signal 对全体等待者生效。
func (x *LockCondition) Broadcast(ctx context.Context) error {
	return x.Signal(ctx)
}

// Wait 释放当前持有的锁，阻塞到下一次 Signal 或 ctx 结束，然后重新获取锁。
// 返回 nil 时调用方再次持有这把锁。重新获取失败时返回该错误，调用方不再持有锁。
func (x *LockCondition) Wait(ctx context.Context) error {
	start, err := x.inner.ns.load(ctx, x.inner.id)
	if err != nil || start == nil {
		if err == nil {
			err = ErrNoPermit
		}
		return err
	}
	seen := start.Signal
	if err := x.lock.UnLock(ctx, x.owner); err != nil {
		return err
	}
	for {
		current, err := x.inner.ns.load(ctx, x.inner.id)
		if err != nil {
			return errors.Join(err, x.relock(ctx))
		}
		if current != nil && current.Signal != seen {
			return x.relock(ctx)
		}
		select {
		case <-ctx.Done():
			relockErr := x.relock(context.WithoutCancel(ctx))
			return errors.Join(ctx.Err(), relockErr)
		case <-x.signal:
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (x *LockCondition) relock(ctx context.Context) error {
	relockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return x.lock.Lock(relockCtx, x.owner)
}
