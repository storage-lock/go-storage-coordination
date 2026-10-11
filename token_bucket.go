package coordination

import (
	"context"
	"time"
)

// TokenBucket 是跨实例共享的令牌桶。
// 补充速率和余额都由存储里的同一条 CAS 记录决定，时间取自命名空间的统一时间源。
type TokenBucket struct {
	ns         *Namespace
	id         string
	capacity   int64
	refillRate time.Duration // 生成一个令牌所需的时间
	now        func() time.Time
}

// TokenBucket 创建容量为 capacity、每 interval 补充一个令牌的桶。
func (x *Namespace) TokenBucket(id string, capacity int, interval time.Duration) (*TokenBucket, error) {
	if capacity <= 0 || interval <= 0 {
		return nil, ErrInvalidCount
	}
	bucket := &TokenBucket{ns: x, id: id, capacity: int64(capacity), refillRate: interval, now: time.Now}
	err := retry(context.Background(), func() error {
		current, err := x.load(context.Background(), id)
		if err != nil || current != nil {
			return err
		}
		return x.swap(context.Background(), id, nil, &state{
			TokensMilli: int64(capacity) * 1000,
			UpdatedAt:   bucket.current(context.Background()),
			Version:     1,
		})
	})
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

// Allow 消耗一个令牌。余额不足返回 ErrRateLimited。
func (x *TokenBucket) Allow(ctx context.Context) error {
	return x.AllowN(ctx, 1)
}

// AllowN 消耗 n 个令牌。n 超过容量或余额不足都返回 ErrRateLimited。
func (x *TokenBucket) AllowN(ctx context.Context, n int) error {
	if n <= 0 || int64(n) > x.capacity {
		return ErrRateLimited
	}
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
		tokens := current.TokensMilli
		elapsed := now - current.UpdatedAt
		if elapsed > 0 {
			refilled := elapsed * 1000 / int64(x.refillRate)
			tokens += refilled
			if cap := x.capacity * 1000; tokens > cap {
				tokens = cap
			}
		}
		need := int64(n) * 1000
		if tokens < need {
			return ErrRateLimited
		}
		next.TokensMilli = tokens - need
		next.UpdatedAt = now
		next.Version++
		return x.ns.swap(ctx, x.id, current, &next)
	})
}

func (x *TokenBucket) current(ctx context.Context) int64 {
	if x.ns.clock != nil {
		if now, err := x.ns.clock.GetTime(ctx); err == nil {
			return now.UnixNano()
		}
	}
	return x.now().UnixNano()
}
