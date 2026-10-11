# go-storage-coordination

在已有分布式锁之外提供跨实例协调原语。所有状态通过 CAS 版本推进，不依赖存储原生 Pub/Sub；等待操作以短轮询感知变化。

## 能力

- `Semaphore`：计数许可，`TryAcquire` 立即失败，`Acquire` 等待归还。
- `Latch`：固定次数 `CountDown` 后释放全部 `Wait`。
- `Condition`：`Signal` 唤醒在调用之后进入 `Wait` 的一方。
- `LockCondition`：绑定一把锁的条件变量。`Wait` 先释放锁，被 `Signal` 唤醒或取消后再重新获取，调用方必须重新检查条件。
- `Namespace`：用命名空间隔离同一存储中的记录键。
- `MultiLock`：按锁 ID 排序获取多把锁，失败时逆序回滚，避免组合加锁死锁。
- `Barrier`：全员到达后同时放行。
- `RateLimiter`：跨实例固定窗口限流。命名空间设置 `SetTimeProvider` 后窗口按统一时间源推进，不再受各实例本地时钟偏差影响。
- `TokenBucket`：跨实例令牌桶，按同一时间源连续补充，`AllowN` 一次消耗多个令牌。

## 示例

```go
ns, err := coordination.NewNamespace("orders", coordination.NewMemoryStore())
limiter, err := ns.RateLimiter("create", 100, time.Second)
if err := limiter.Allow(ctx); err != nil {
    return err
}

multi, err := coordination.NewMultiLock(map[string]coordination.Lock{
    "inventory": inventoryLock,
    "payment":   paymentLock,
})
if err := multi.Lock(ctx, ownerID); err != nil {
    return err
}
defer multi.Unlock(context.Background(), ownerID)
```

`Lock` 接口只要求 `Lock` 和 `UnLock`，`*storage_lock.StorageLock` 可以直接传入。生产存储使用 `NewStorageAdapter`。
