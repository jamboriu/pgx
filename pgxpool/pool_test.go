package pgxpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colmev080/pgx/pgxpool"
)

// TestMaxConnsNeverExceededUnderRecovery verifies that during a DB recovery
// with high contention (100 concurrent goroutines), the sum of active/established
// connections and in-flight dials NEVER exceeds MaxConns.
func TestMaxConnsNeverExceededUnderRecovery(t *testing.T) {
	const maxConns int32 = 5
	const numGoroutines = 100

	var currentDials int32
	var maxObservedDials int32

	dialFunc := func(ctx context.Context) (*pgxpool.Conn, error) {
		active := atomic.AddInt32(&currentDials, 1)
		defer atomic.AddInt32(&currentDials, -1)

		for {
			max := atomic.LoadInt32(&maxObservedDials)
			if active > max {
				if atomic.CompareAndSwapInt32(&maxObservedDials, max, active) {
					break
				}
			} else {
				break
			}
		}

		// Simula um atraso na conexão física (ex: handshake / I/O de rede)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}

		return &pgxpool.Conn{}, nil
	}

	pool := pgxpool.NewPool(maxConns, dialFunc)

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- err
				return
			}
			// Retém a conexão brevemente e libera
			time.Sleep(10 * time.Millisecond)
			pool.Release(conn)
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("Erro durante Acquire em concorrência: %v", err)
	}

	if maxObservedDials > maxConns {
		t.Fatalf("VIOLAÇÃO DE MAXCONNS! Máximo de conexões simultâneas discando foi %d, limite era %d", maxObservedDials, maxConns)
	}

	t.Logf("✅ SUCESSO: 100 Goroutines executadas. Máximo de conexões simultâneas físicas registradas foi %d (limite = %d)", maxObservedDials, maxConns)
}

// TestInFlightCounterDecrementedOnError verifies that if connection creation fails,
// the inFlightConns counter is decremented correctly to avoid pool starvation.
func TestInFlightCounterDecrementedOnError(t *testing.T) {
	const maxConns int32 = 2

	dialErr := errors.New("network outage")
	dialFunc := func(ctx context.Context) (*pgxpool.Conn, error) {
		return nil, dialErr
	}

	pool := pgxpool.NewPool(maxConns, dialFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := pool.Acquire(ctx)
	if !errors.Is(err, dialErr) {
		t.Fatalf("Esperava erro %v, veio: %v", dialErr, err)
	}

	if inFlight := pool.InFlightCount(); inFlight != 0 {
		t.Fatalf("Esperava inFlightCount = 0 após erro, mas veio: %d", inFlight)
	}
}
