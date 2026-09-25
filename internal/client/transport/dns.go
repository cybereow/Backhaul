package transport

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
)

type DnsTransport struct {
	config          *DnsConfig
	parentctx       context.Context
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *logrus.Logger
	controlChannel  net.Conn
	usageMonitor    *web.Usage
	restartMutex    sync.Mutex
	poolConnections int32
	loadConnections int32
	controlFlow     chan struct{}
}

type DnsConfig struct {
	RemoteAddr     string
	DNSDomain      string
	Token          string
	SnifferLog     string
	TunnelStatus   string
	KeepAlive      time.Duration
	RetryInterval  time.Duration
	DialTimeOut    time.Duration
	ConnPoolSize   int
	WebPort        int
	Nodelay        bool
	Sniffer        bool
	AggressivePool bool
	MSS            int
	SO_RCVBUF      int
	SO_SNDBUF      int
}

func NewDNSClient(parentCtx context.Context, config *DnsConfig, logger *logrus.Logger) *DnsTransport {
	ctx, cancel := context.WithCancel(parentCtx)

	client := &DnsTransport{
		config:          config,
		parentctx:       parentCtx,
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger,
		controlChannel:  nil,
		usageMonitor:    web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		poolConnections: 0,
		loadConnections: 0,
		controlFlow:     make(chan struct{}, 100),
	}

	return client
}

func (c *DnsTransport) Start() {
	if c.config.WebPort > 0 {
		go c.usageMonitor.Monitor()
	}

	c.config.TunnelStatus = "Disconnected (DNS)"

	go c.channelDialer()
}

func (c *DnsTransport) Restart() {
	if !c.restartMutex.TryLock() {
		c.logger.Warn("client is already restarting")
		return
	}
	defer c.restartMutex.Unlock()

	c.logger.Info("restarting client...")

	level := c.logger.Level
	c.logger.SetLevel(logrus.FatalLevel)

	if c.cancel != nil {
		c.cancel()
	}

	if c.controlChannel != nil {
		c.controlChannel.Close()
	}

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(c.parentctx)
	c.ctx = ctx
	c.cancel = cancel

	c.controlChannel = nil
	c.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", c.config.WebPort), ctx, c.config.SnifferLog, c.config.Sniffer, &c.config.TunnelStatus, c.logger)
	c.config.TunnelStatus = ""
	c.poolConnections = 0
	c.loadConnections = 0
	c.controlFlow = make(chan struct{}, 100)

	c.logger.SetLevel(level)

	go c.Start()
}

func (c *DnsTransport) channelDialer() {
	c.logger.Info("attempting to establish a new control channel connection via DNS...")

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			conn, err := network.DNSDialer(c.ctx, c.config.RemoteAddr, c.config.DNSDomain, c.config.DialTimeOut)
			if err != nil {
				c.logger.Errorf("channel dialer: %v", err)
				time.Sleep(c.config.RetryInterval)
				continue
			}

			err = utils.SendBinaryTransportString(conn, c.config.Token, utils.SG_Chan)
			if err != nil {
				c.logger.Errorf("failed to send security token: %v", err)
				conn.Close()
				continue
			}

			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				c.logger.Errorf("failed to set read deadline: %v", err)
				conn.Close()
				continue
			}

			message, _, err := utils.ReceiveBinaryTransportString(conn)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					c.logger.Warn("timeout while waiting for control channel response")
				} else {
					c.logger.Errorf("failed to receive control channel response: %v", err)
				}
				conn.Close()
				time.Sleep(c.config.RetryInterval)
				continue
			}

			conn.SetReadDeadline(time.Time{})

			if message == c.config.Token {
				c.controlChannel = conn
				c.logger.Info("control channel established successfully via DNS")

				c.config.TunnelStatus = "Connected (DNS)"
				go c.poolMaintainer()
				go c.channelHandler()

				return
			} else {
				c.logger.Errorf("invalid token received. Expected: %s, Received: %s. Retrying...", c.config.Token, message)
				conn.Close()
				time.Sleep(c.config.RetryInterval)
				continue
			}
		}
	}
}

func (c *DnsTransport) poolMaintainer() {
	for i := 0; i < c.config.ConnPoolSize; i++ {
		go c.tunnelDialer()
	}

	a := 4
	b := 5
	x := 3
	y := 4.0

	if c.config.AggressivePool {
		c.logger.Info("aggressive pool management enabled")
		a = 1
		b = 2
		x = 0
		y = 0.75
	}

	tickerPool := time.NewTicker(time.Second * 1)
	defer tickerPool.Stop()

	tickerLoad := time.NewTicker(time.Second * 10)
	defer tickerLoad.Stop()

	newPoolSize := c.config.ConnPoolSize
	var poolConnectionsSum int32 = 0

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-tickerPool.C:
			atomic.AddInt32(&poolConnectionsSum, atomic.LoadInt32(&c.poolConnections))

		case <-tickerLoad.C:
			loadConnections := (int(atomic.LoadInt32(&c.loadConnections)) + 9) / 10
			atomic.StoreInt32(&c.loadConnections, 0)

			poolConnectionsAvg := (int(atomic.LoadInt32(&poolConnectionsSum)) + 9) / 10
			atomic.StoreInt32(&poolConnectionsSum, 0)

			if (loadConnections + a) > poolConnectionsAvg*b {
				c.logger.Debugf("increasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize+1, poolConnectionsAvg, loadConnections)
				newPoolSize++
				go c.tunnelDialer()
			} else if float64(loadConnections+x) < float64(poolConnectionsAvg)*y && newPoolSize > c.config.ConnPoolSize {
				c.logger.Debugf("decreasing pool size: %d -> %d, avg pool conn: %d, avg load conn: %d", newPoolSize, newPoolSize-1, poolConnectionsAvg, loadConnections)
				newPoolSize--
				c.controlFlow <- struct{}{}
			}
		}
	}
}

func (c *DnsTransport) channelHandler() {
	msgChan := make(chan byte, 1000)

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				msg, err := utils.ReceiveBinaryByte(c.controlChannel)
				if err != nil {
					if c.cancel != nil {
						c.logger.Error("failed to read from control channel. ", err)
						go c.Restart()
					}
					return
				}
				msgChan <- msg
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			_ = utils.SendBinaryByte(c.controlChannel, utils.SG_Closed)
			return

		case msg := <-msgChan:
			switch msg {
			case utils.SG_Chan:
				atomic.AddInt32(&c.loadConnections, 1)
				select {
				case <-c.controlFlow:
				default:
					c.logger.Debug("channel signal received, initiating tunnel dialer")
					go c.tunnelDialer()
				}
			case utils.SG_HB:
				c.logger.Debug("heartbeat signal received successfully")
			case utils.SG_Closed:
				c.logger.Warn("control channel has been closed by the server")
				go c.Restart()
				return
			case utils.SG_RTT:
				err := utils.SendBinaryByte(c.controlChannel, utils.SG_RTT)
				if err != nil {
					c.logger.Error("failed to send RTT signal, restarting client: ", err)
					go c.Restart()
					return
				}
			default:
				c.logger.Errorf("unexpected response from channel: %v.", msg)
				go c.Restart()
				return
			}
		}
	}
}

func (c *DnsTransport) tunnelDialer() {
	c.logger.Debugf("initiating new connection to tunnel server via DNS at %s", c.config.RemoteAddr)

	conn, err := network.DNSDialer(c.ctx, c.config.RemoteAddr, c.config.DNSDomain, c.config.DialTimeOut)
	if err != nil {
		c.logger.Error("tunnel server dialer: ", err)
		return
	}

	atomic.AddInt32(&c.poolConnections, 1)
	remoteAddr, transport, err := utils.ReceiveBinaryTransportString(conn)
	atomic.AddInt32(&c.poolConnections, -1)

	if err != nil {
		c.logger.Debugf("failed to receive port from tunnel connection: %v", err)
		conn.Close()
		return
	}

	port, resolvedAddr, err := network.ResolveRemoteAddr(remoteAddr)
	if err != nil {
		c.logger.Infof("failed to resolve remote port: %v", err)
		conn.Close()
		return
	}

	switch transport {
	case utils.SG_TCP:
		c.localDialer(conn, resolvedAddr, port)
	case utils.SG_UDP:
		UDPDialer(conn, resolvedAddr, c.logger, c.usageMonitor, port, c.config.Sniffer)
	default:
		c.logger.Error("undefined transport. close the connection.")
		conn.Close()
	}
}

func (c *DnsTransport) localDialer(tcpConn net.Conn, resolvedAddr string, port int) {
	var sendBuf, recvBuf int

	if strings.Contains(resolvedAddr, "127.0.0.1") {
		sendBuf = 32 * 1024
		recvBuf = 32 * 1024
	} else {
		sendBuf = c.config.SO_SNDBUF
		recvBuf = c.config.SO_RCVBUF
	}

	localConnection, err := network.TcpDialer(c.ctx, resolvedAddr, "", c.config.DialTimeOut, c.config.KeepAlive, true, 1, recvBuf, sendBuf, c.config.MSS)
	if err != nil {
		c.logger.Errorf("local dialer: %v", err)
		tcpConn.Close()
		return
	}

	c.logger.Debugf("connected to local address %s successfully", resolvedAddr)
	handlers.TCPConnectionHandler(c.ctx, false, tcpConn, localConnection, c.logger, c.usageMonitor, port, c.config.Sniffer)
}
