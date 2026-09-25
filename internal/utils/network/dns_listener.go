package network

import (
	"encoding/base32"
	"encoding/base64"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/sirupsen/logrus"
)

type DNSListener struct {
	bindAddr string
	domain   string
	server   *dns.Server
	logger   *logrus.Logger

	conns     map[string]*DNSConn
	connsLock sync.Mutex

	acceptChan chan net.Conn
	closeChan  chan struct{}
}

func NewDNSListener(bindAddr, domain string, logger *logrus.Logger) *DNSListener {
	return &DNSListener{
		bindAddr:   bindAddr,
		domain:     domain,
		logger:     logger,
		conns:      make(map[string]*DNSConn),
		acceptChan: make(chan net.Conn, 1024),
		closeChan:  make(chan struct{}),
	}
}

func (l *DNSListener) Start() error {
	l.server = &dns.Server{Addr: l.bindAddr, Net: "udp"}
	dns.HandleFunc(l.domain+".", l.handleDNSRequest)

	go func() {
		l.logger.Infof("starting DNS listener on %s for domain %s", l.bindAddr, l.domain)
		err := l.server.ListenAndServe()
		if err != nil {
			l.logger.Errorf("DNS server stopped: %v", err)
		}
	}()

	return nil
}

func (l *DNSListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.acceptChan:
		return conn, nil
	case <-l.closeChan:
		return nil, net.ErrClosed
	}
}

func (l *DNSListener) Close() error {
	close(l.closeChan)
	if l.server != nil {
		return l.server.Shutdown()
	}
	return nil
}

func (l *DNSListener) Addr() net.Addr {
	return &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 53,
	}
}

func (l *DNSListener) handleDNSRequest(w dns.ResponseWriter, r *dns.Msg) {
	msg := new(dns.Msg)
	msg.SetReply(r)
	msg.Authoritative = true

	for _, q := range r.Question {
		qName := strings.ToLower(q.Name)
		if q.Qtype == dns.TypeTXT && strings.HasSuffix(qName, l.domain+".") {
			parts := strings.Split(qName, ".")
			// Format: <data>.<connID>.<domain>.
			// Note: l.domain may have multiple parts.
			domainPartsCount := len(strings.Split(l.domain, "."))

			if len(parts) >= domainPartsCount + 3 { // +3 for data, connID, and empty string at end (due to trailing dot)
				connIDIndex := len(parts) - domainPartsCount - 2
				dataIndex := connIDIndex - 1

				connID := parts[connIDIndex]
				dataStr := parts[dataIndex]

				conn := l.getOrCreateConn(connID)

				if dataStr != "poll" {
					// It's data
					decodedData, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(dataStr))
					if err == nil {
						conn.PushUpstreamData(decodedData)
					} else {
						l.logger.Debugf("failed to decode base32 data: %v", err)
					}
				}

				// Check if there is data to send back
				var responseData []byte
				select {
				case data := <-conn.downstreamChan:
					responseData = data
				case <-time.After(50 * time.Millisecond): // Small delay to allow data to accumulate
					select {
					case data := <-conn.downstreamChan:
						responseData = data
					default:
						// No data
					}
				}

				if len(responseData) > 0 {
					encodedResp := base64.StdEncoding.EncodeToString(responseData)
					txt, _ := dns.NewRR(q.Name + " 0 IN TXT \"" + encodedResp + "\"")
					if txt != nil {
						msg.Answer = append(msg.Answer, txt)
					}
				}
			}
		}
	}

	w.WriteMsg(msg)
}

func (l *DNSListener) getOrCreateConn(connID string) *DNSConn {
	l.connsLock.Lock()
	defer l.connsLock.Unlock()

	if conn, exists := l.conns[connID]; exists {
		return conn
	}

	// Create new connection
	sendResp := func(id string, data []byte) error {
		l.connsLock.Lock()
		c, ok := l.conns[id]
		l.connsLock.Unlock()
		if ok {
			c.PushDownstreamData(data) // Queue for the client to poll
		}
		return nil
	}

	conn := NewDNSConnServer(connID, l.domain, sendResp)
	l.conns[connID] = conn

	l.acceptChan <- conn

	return conn
}
