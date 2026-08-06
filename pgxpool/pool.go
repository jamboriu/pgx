package pgxpool

import (
	"context"
	"sync"
	"sync/atomic"
)

type Pool struct {
	// ... existing fields
	maxConns      int32
	conns         []*conn
	inFlightConns int32
	mu            sync.Mutex
	// ...
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	// Check if we can create a new connection
	if len(p.conns) + int(atomic.LoadInt32(&p.inFlightConns)) < int(p.maxConns) {
		atomic.AddInt32(&p.inFlightConns, 1)
		p.mu.Unlock()

		conn, err := p.createNewConn(ctx)
		atomic.AddInt32(&p.inFlightConns, -1)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	p.mu.Unlock()

	// Wait for existing connection or retry logic...
	return p.waitForConn(ctx)
}