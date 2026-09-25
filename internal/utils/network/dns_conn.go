package network

import (
	"encoding/base32"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

var (
	ErrDNSConnClosed = errors.New("dns connection closed")
)

type DNSConn struct {
	connID     string
	domain     string
	isServer   bool
	serverAddr string // For client: where to send queries
	dnsClient  *dns.Client

	// For read buffer
	readBuf  []byte
	readLock sync.Mutex

	// Server to client queue
	downstreamChan chan []byte
	// Client to server queue
	upstreamChan chan []byte

	// To signal closure
	closeChan chan struct{}
	closed    bool
	closeLock sync.Mutex

	readDeadline  time.Time
	writeDeadline time.Time

	// Server callback to send response
	sendResponse func(connID string, data []byte) error
}

func NewDNSConnClient(connID, domain, serverAddr string) *DNSConn {
	return &DNSConn{
		connID:         connID,
		domain:         domain,
		isServer:       false,
		serverAddr:     serverAddr,
		dnsClient:      new(dns.Client),
		downstreamChan: make(chan []byte, 100),
		upstreamChan:   make(chan []byte, 100),
		closeChan:      make(chan struct{}),
	}
}

func NewDNSConnServer(connID, domain string, sendResp func(string, []byte) error) *DNSConn {
	return &DNSConn{
		connID:         connID,
		domain:         domain,
		isServer:       true,
		downstreamChan: make(chan []byte, 100),
		upstreamChan:   make(chan []byte, 100),
		closeChan:      make(chan struct{}),
		sendResponse:   sendResp,
	}
}

func (c *DNSConn) Read(b []byte) (n int, err error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()

	// If we have data in the buffer, read it first
	if len(c.readBuf) > 0 {
		n = copy(b, c.readBuf)
		c.readBuf = c.readBuf[n:]
		return n, nil
	}

	// If client, read from downstream. If server, read from upstream.
	readChannel := c.downstreamChan
	if c.isServer {
		readChannel = c.upstreamChan
	}

	// For client, if read channel is empty, we need to poll
	if !c.isServer {
		select {
		case data := <-readChannel:
			n = copy(b, data)
			c.readBuf = data[n:]
			return n, nil
		case <-c.closeChan:
			return 0, io.EOF
		default:
			// Poll the server
			err := c.pollServer()
			if err != nil {
				return 0, err
			}
		}
	}

	var timer *time.Timer
	var timeoutChan <-chan time.Time

	if !c.readDeadline.IsZero() {
		timer = time.NewTimer(time.Until(c.readDeadline))
		defer timer.Stop()
		timeoutChan = timer.C
	}

	select {
	case data := <-readChannel:
		n = copy(b, data)
		c.readBuf = data[n:]
		return n, nil
	case <-c.closeChan:
		return 0, io.EOF
	case <-timeoutChan:
		return 0, net.ErrClosed // Or timeout error
	}
}

func (c *DNSConn) Write(b []byte) (n int, err error) {
	if c.isClosed() {
		return 0, ErrDNSConnClosed
	}

	if c.isServer {
		// Server writes data back to the client via DNS response
		err = c.sendResponse(c.connID, b)
		if err != nil {
			return 0, err
		}
		return len(b), nil
	}

	// Client writes data to the server via DNS query (TXT record)
	// Base32 encode the data to be safe for subdomains
	// Max DNS label size is 63 bytes. Base32 inflates 8/5.
	// We might need to split into multiple queries if large, but for simplicity let's assume
	// smux or transport chunks it, or we split it here.

	// Split data into chunks that fit in a DNS query
	// Max domain length is 255. Domain is <base32>.<connID>.<domain>
	// Max label length is 63.

	maxBase32Len := 63 // One label max
	maxRawLen := (maxBase32Len * 5) / 8 // approx 39 bytes

	written := 0
	for len(b) > 0 {
		chunkSize := maxRawLen
		if len(b) < chunkSize {
			chunkSize = len(b)
		}
		chunk := b[:chunkSize]
		b = b[chunkSize:]

		encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(chunk)
		qName := strings.ToLower(encoded) + "." + c.connID + "." + c.domain + "."

		msg := new(dns.Msg)
		msg.SetQuestion(qName, dns.TypeTXT)

		resp, _, err := c.dnsClient.Exchange(msg, c.serverAddr)
		if err != nil {
			return written, err
		}

		c.handleResponse(resp)
		written += len(chunk)
	}

	return written, nil
}

func (c *DNSConn) pollServer() error {
	qName := "poll." + c.connID + "." + c.domain + "."
	msg := new(dns.Msg)
	msg.SetQuestion(qName, dns.TypeTXT)

	resp, _, err := c.dnsClient.Exchange(msg, c.serverAddr)
	if err != nil {
		return err
	}
	c.handleResponse(resp)
	return nil
}

func (c *DNSConn) handleResponse(resp *dns.Msg) {
	if resp != nil {
		for _, answer := range resp.Answer {
			if txt, ok := answer.(*dns.TXT); ok {
				for _, txtData := range txt.Txt {
					// Base64 decode
					decoded, err := base64.StdEncoding.DecodeString(txtData)
					if err == nil && len(decoded) > 0 {
						c.PushDownstreamData(decoded)
					}
				}
			}
		}
	}
}

func (c *DNSConn) PushDownstreamData(data []byte) {
	if !c.isClosed() {
		c.downstreamChan <- data
	}
}

func (c *DNSConn) PushUpstreamData(data []byte) {
	if !c.isClosed() {
		c.upstreamChan <- data
	}
}

func (c *DNSConn) isClosed() bool {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	return c.closed
}

func (c *DNSConn) Close() error {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	if !c.closed {
		c.closed = true
		close(c.closeChan)
	}
	return nil
}

func (c *DNSConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "dns-local", Net: "dns"}
}

func (c *DNSConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "dns-remote", Net: "dns"}
}

func (c *DNSConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}

func (c *DNSConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

func (c *DNSConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}
