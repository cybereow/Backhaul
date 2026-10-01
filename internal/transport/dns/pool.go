package dnsx

import (
	"context"
	"sync"
	"time"

	"github.com/miekg/dns"
)

const (
	poolMaxIdle = 32              // idle conns kept per resolver
	poolMaxAge  = 6 * time.Second // resolvers drop idle TCP conns after a few seconds, often silently; do not reuse old ones
	// pooledTimeout bounds an attempt on a REUSED conn. A conn the resolver dropped
	// without a RST swallows the query and the caller would wait the whole exchange
	// timeout (seconds) before retrying; a reused conn that has not answered in this
	// long is treated as dead and the query goes out on a fresh one.
	pooledTimeout = 1200 * time.Millisecond
)

// connPool keeps idle TCP conns to resolvers so a query does not pay a TCP
// handshake (and a fresh resolver-side connection) every time. Each conn carries
// one query at a time; concurrency comes from several conns.
type connPool struct {
	mu   sync.Mutex
	idle map[string][]pooledConn
}

type pooledConn struct {
	c    *dns.Conn
	when time.Time
}

func newConnPool() *connPool { return &connPool{idle: map[string][]pooledConn{}} }

func (p *connPool) get(addr string) *dns.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	l := p.idle[addr]
	for len(l) > 0 {
		pc := l[len(l)-1]
		l = l[:len(l)-1]
		if time.Since(pc.when) < poolMaxAge {
			p.idle[addr] = l
			return pc.c
		}
		pc.c.Close()
	}
	p.idle[addr] = l
	return nil
}

func (p *connPool) put(addr string, c *dns.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle[addr]) >= poolMaxIdle {
		c.Close()
		return
	}
	p.idle[addr] = append(p.idle[addr], pooledConn{c, time.Now()})
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.idle {
		for _, pc := range l {
			pc.c.Close()
		}
	}
	p.idle = map[string][]pooledConn{}
}

// exchange runs one query over a pooled conn; a stale pooled conn that fails is
// retried once on a fresh one. Failed conns are closed, good ones go back.
func (p *connPool) exchange(ctx context.Context, client *dns.Client, msg *dns.Msg, addr string) (*dns.Msg, time.Duration, error) {
	if c := p.get(addr); c != nil {
		pctx, cancel := context.WithTimeout(ctx, pooledTimeout)
		reply, rtt, err := client.ExchangeWithConnContext(pctx, msg, c)
		cancel()
		if err == nil {
			p.put(addr, c)
			return reply, rtt, nil
		}
		c.Close() // dead, or too slow to trust: fall through to a fresh conn
	}
	c, err := client.DialContext(ctx, addr)
	if err != nil {
		return nil, 0, err
	}
	reply, rtt, err := client.ExchangeWithConnContext(ctx, msg, c)
	if err != nil {
		c.Close()
		return nil, rtt, err
	}
	p.put(addr, c)
	return reply, rtt, nil
}
