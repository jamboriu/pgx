package pgxpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrMaxConnsReached = errors.New("max connections reached")
	ErrConnClosed      = errors.New("connection closed")
)

type Conn struct {
	id int
}

type Pool struct {
	maxConns      int32
	conns         []*Conn
	inFlightConns int32
	mu            sync.Mutex
	cond          *sync.Cond
	dialFunc      func(ctx context.Context) (*Conn, error)
	connCounter   int32
}

func NewPool(maxConns int32, dialFunc func(ctx context.Context) (*Conn, error)) *Pool {
	p := &Pool{
		maxConns: maxConns,
		conns:    make([]*Conn, 0, maxConns),
		dialFunc: dialFunc,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()

	for {
		// 1. Tentar pegar conexão já estabelecida e ociosa no pool
		if len(p.conns) > 0 {
			conn := p.conns[len(p.conns)-1]
			p.conns = p.conns[:len(p.conns)-1]
			p.mu.Unlock()
			return conn, nil
		}

		// 2. Se a soma de (estabelecidas + em progresso/in-flight) for menor que MaxConns, abre nova vaga
		currentInFlight := atomic.LoadInt32(&p.inFlightConns)
		if int32(len(p.conns))+currentInFlight < p.maxConns {
			atomic.AddInt32(&p.inFlightConns, 1)
			p.mu.Unlock()

			// Dispara discagem física (I/O) fora da trava principal
			conn, err := p.createNewConn(ctx)
			atomic.AddInt32(&p.inFlightConns, -1)

			p.mu.Lock()
			// Acorda goroutines aguardando por alteração de estado no pool
			p.cond.Broadcast()

			if err != nil {
				p.mu.Unlock()
				return nil, err
			}

			p.mu.Unlock()
			return conn, nil
		}

		// 3. Se atingiu o limite de capacidade (estabelecidas + in-flight >= MaxConns), aguarda
		select {
		case <-ctx.Done():
			p.mu.Unlock()
			return nil, ctx.Err()
		default:
		}

		// Aguarda sinal de liberação ou finalização de conexão física
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				p.mu.Lock()
				p.cond.Broadcast()
				p.mu.Unlock()
			case <-done:
			}
		}()

		p.cond.Wait()
		close(done)

		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
	}
}

func (p *Pool) Release(conn *Conn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.cond.Signal()
	p.mu.Unlock()
}

func (p *Pool) createNewConn(ctx context.Context) (*Conn, error) {
	if p.dialFunc != nil {
		return p.dialFunc(ctx)
	}
	// Fallback/Default Dial
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Millisecond):
		id := atomic.AddInt32(&p.connCounter, 1)
		return &Conn{id: int(id)}, nil
	}
}

func (p *Pool) TotalEstablished() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

func (p *Pool) InFlightCount() int32 {
	return atomic.LoadInt32(&p.inFlightConns)
}