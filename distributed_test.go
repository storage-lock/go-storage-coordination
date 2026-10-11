package coordination

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type scriptLock struct {
	id       string
	order    *[]string
	mu       *sync.Mutex
	held     map[string]int
	failLock bool
}

func (x *scriptLock) Lock(_ context.Context, owner string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	*x.order = append(*x.order, "lock:"+x.id)
	if x.failLock {
		return errors.New("lock denied")
	}
	x.held[owner]++
	return nil
}

func (x *scriptLock) UnLock(_ context.Context, owner string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	*x.order = append(*x.order, "unlock:"+x.id)
	if x.held[owner] == 0 {
		return errors.New("not held")
	}
	x.held[owner]--
	return nil
}

func TestMultiLockOrdersAndRollsBack(t *testing.T) {
	var order []string
	mu := &sync.Mutex{}
	held := map[string]int{}
	locks := map[string]Lock{
		"c": &scriptLock{id: "c", order: &order, mu: mu, held: held},
		"a": &scriptLock{id: "a", order: &order, mu: mu, held: held},
		"b": &scriptLock{id: "b", order: &order, mu: mu, held: held, failLock: true},
	}
	multi, err := NewMultiLock(locks)
	if err != nil {
		t.Fatal(err)
	}
	err = multi.Lock(context.Background(), "owner")
	if err == nil || err.Error() != "lock denied" {
		t.Fatalf("lock error=%v", err)
	}
	want := []string{"lock:a", "lock:b", "unlock:a"}
	if len(order) != len(want) {
		t.Fatalf("order=%v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v", order)
		}
	}
	if held["owner"] != 0 {
		t.Fatalf("rollback leaked hold: %d", held["owner"])
	}
}

func TestBarrierReleasesAllParties(t *testing.T) {
	ns, err := NewNamespace("fleet", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := ns.Barrier("deploy", 3)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var ready sync.WaitGroup
	errCh := make(chan error, 3)
	for i := 0; i < 3; i++ {
		ready.Add(1)
		go func() {
			ready.Done()
			errCh <- barrier.Arrive(ctx)
		}()
	}
	ready.Wait()
	for i := 0; i < 3; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRateLimiterWindow(t *testing.T) {
	ns, err := NewNamespace("api", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := ns.RateLimiter("write", 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	current := time.Unix(0, 1_000)
	limiter.now = func() time.Time { return current }
	ctx := context.Background()
	if err := limiter.Allow(ctx); err != nil || limiter.Allow(ctx) != nil {
		t.Fatal("first window should allow two calls")
	}
	if err := limiter.Allow(ctx); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third call=%v", err)
	}
	current = current.Add(time.Second)
	if err := limiter.Allow(ctx); err != nil {
		t.Fatalf("new window=%v", err)
	}
}
