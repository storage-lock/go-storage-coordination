package coordination

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSemaphoreLatchConditionAndNamespace(t *testing.T) {
	ctx := context.Background()
	left, err := NewNamespace("tenant-a", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewNamespace("tenant-b", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}

	semaphore, err := left.Semaphore("jobs", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := semaphore.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := semaphore.TryAcquire(ctx); !errors.Is(err, ErrNoPermit) {
		t.Fatalf("second acquire=%v", err)
	}
	if err := semaphore.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := semaphore.TryAcquire(ctx); err != nil {
		t.Fatal(err)
	}

	latch, err := left.Latch("batch", 2)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- latch.Wait(ctx) }()
	if err := latch.CountDown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("latch returned before zero")
	case <-time.After(20 * time.Millisecond):
	}
	if err := latch.CountDown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("latch wait timed out")
	}

	condition, err := left.Condition("ready")
	if err != nil {
		t.Fatal(err)
	}
	var waiting sync.WaitGroup
	waiting.Add(1)
	waitDone := make(chan error, 1)
	go func() {
		waiting.Done()
		waitDone <- condition.Wait(ctx)
	}()
	waiting.Wait()
	time.Sleep(20 * time.Millisecond)
	if err := condition.Signal(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("condition wait timed out")
	}

	other, err := right.Semaphore("jobs", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.TryAcquire(ctx); err != nil {
		t.Fatalf("namespace isolation failed: %v", err)
	}
	if _, err := NewNamespace("", NewMemoryStore()); !errors.Is(err, ErrNamespaceEmpty) {
		t.Fatalf("empty namespace=%v", err)
	}
}
