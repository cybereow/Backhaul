package transport

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/musix/backhaul/internal/utils"
	"github.com/musix/backhaul/internal/utils/handlers"
	"github.com/musix/backhaul/internal/utils/network"
	"github.com/musix/backhaul/internal/web"

	"github.com/sirupsen/logrus"
)

type DnsTransport struct {
	config         *DnsConfig
	parentctx      context.Context
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *logrus.Logger
	tunnelChannel  chan net.Conn
	localChannel   chan LocalTCPConn
	reqNewConnChan chan struct{}
	controlChannel net.Conn
	restartMutex   sync.Mutex
	usageMonitor   *web.Usage
	rtt            int64 // in ms, for UDP
	dnsListener    *network.DNSListener
}

type DnsConfig struct {
	BindAddr      string
	DNSDomain     string
	Token         string
	SnifferLog    string
	TunnelStatus  string
	Ports         []string
	Nodelay       bool
	Sniffer       bool
	KeepAlive     time.Duration
	Heartbeat     time.Duration // in seconds
	ChannelSize   int
	WebPort       int
	AcceptUDP     bool
	MSS           int
	SO_RCVBUF     int
	SO_SNDBUF     int
	ProxyProtocol bool
}

func NewDNSServer(parentCtx context.Context, config *DnsConfig, logger *logrus.Logger) *DnsTransport {
	ctx, cancel := context.WithCancel(parentCtx)

	server := &DnsTransport{
		config:         config,
		parentctx:      parentCtx,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		tunnelChannel:  make(chan net.Conn, config.ChannelSize),
		localChannel:   make(chan LocalTCPConn, config.ChannelSize),
		reqNewConnChan: make(chan struct{}, config.ChannelSize),
		controlChannel: nil, // will be set when a control connection is established
		usageMonitor:   web.NewDataStore(fmt.Sprintf(":%v", config.WebPort), ctx, config.SnifferLog, config.Sniffer, &config.TunnelStatus, logger),
		rtt:            0,
	}

	return server
}

func (s *DnsTransport) Start() {
	s.config.TunnelStatus = "Disconnected (DNS)"

	if s.config.WebPort > 0 {
		go s.usageMonitor.Monitor()
	}

	go s.tunnelListener()

	s.channelHandshake()

	if s.controlChannel != nil {
		s.config.TunnelStatus = "Connected (DNS)"

		numCPU := runtime.NumCPU()
		if numCPU > 4 {
			numCPU = 4 // Max allowed handler is 4
		}

		go s.parsePortMappings()
		go s.channelHandler()

		s.logger.Infof("starting %d handle loops on each CPU thread", numCPU)

		for i := 0; i < numCPU; i++ {
			go s.handleLoop()
		}
	}
}

func (s *DnsTransport) Restart() {
	if !s.restartMutex.TryLock() {
		s.logger.Warn("server restart already in progress, skipping restart attempt")
		return
	}
	defer s.restartMutex.Unlock()

	s.logger.Info("restarting server...")

	level := s.logger.Level
	s.logger.SetLevel(logrus.FatalLevel)

	if s.cancel != nil {
		s.cancel()
	}

	if s.controlChannel != nil {
		s.controlChannel.Close()
	}
	if s.dnsListener != nil {
		s.dnsListener.Close()
	}

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithCancel(s.parentctx)
	s.ctx = ctx
	s.cancel = cancel

	s.tunnelChannel = make(chan net.Conn, s.config.ChannelSize)
	s.localChannel = make(chan LocalTCPConn, s.config.ChannelSize)
	s.reqNewConnChan = make(chan struct{}, s.config.ChannelSize)
	s.usageMonitor = web.NewDataStore(fmt.Sprintf(":%v", s.config.WebPort), ctx, s.config.SnifferLog, s.config.Sniffer, &s.config.TunnelStatus, s.logger)
	s.config.TunnelStatus = ""
	s.controlChannel = nil
	s.dnsListener = nil

	s.logger.SetLevel(level)

	go s.Start()
}

func (s *DnsTransport) channelHandshake() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case conn := <-s.tunnelChannel:
			message, _, err := utils.ReceiveBinaryTransportString(conn)
			if err != nil {
				s.logger.Error("handshake failed to receive token: ", err)
				conn.Close()
				continue
			}

			if message == s.config.Token {
				err = utils.SendBinaryTransportString(conn, s.config.Token, utils.SG_Chan)
				if err != nil {
					s.logger.Error("failed to send control channel token")
					conn.Close()
					continue
				}

				s.controlChannel = conn
				s.logger.Info("control channel established successfully")
				return
			} else {
				s.logger.Warnf("invalid token received from %s", conn.RemoteAddr().String())
				conn.Close()
				continue
			}
		}
	}
}

func (s *DnsTransport) channelHandler() {
	messageChan := make(chan byte, 1000)

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				message, err := utils.ReceiveBinaryByte(s.controlChannel)
				if err != nil {
					if s.cancel != nil {
						s.logger.Error("failed to read from control channel. ", err)
						go s.Restart()
					}
					return
				}
				messageChan <- message
			}
		}
	}()

	ticker := time.NewTicker(s.config.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			_ = utils.SendBinaryByte(s.controlChannel, utils.SG_Closed)
			return

		case <-s.reqNewConnChan:
			err := utils.SendBinaryByte(s.controlChannel, utils.SG_Chan)
			if err != nil {
				s.logger.Error("failed to send request new connection signal. ", err)
				go s.Restart()
				return
			}

		case <-ticker.C:
			err := utils.SendBinaryByte(s.controlChannel, utils.SG_HB)
			if err != nil {
				s.logger.Error("failed to send heartbeat signal")
				go s.Restart()
				return
			}
			s.logger.Trace("heartbeat signal sent successfully")

		case message, ok := <-messageChan:
			if !ok {
				s.logger.Error("channel closed, likely due to an error in DNS read")
				return
			}

			if message == utils.SG_Closed {
				s.logger.Warn("control channel has been closed by the client")
				go s.Restart()
				return
			} else if message == utils.SG_RTT {
				s.logger.Infof("Round Trip Time (RTT): %d ms", s.rtt)
			}
		}
	}
}

func (s *DnsTransport) tunnelListener() {
	l := network.NewDNSListener(s.config.BindAddr, s.config.DNSDomain, s.logger)
	if err := l.Start(); err != nil {
		s.logger.Fatalf("failed to start DNS listener on %s: %v", s.config.BindAddr, err)
		return
	}
	s.dnsListener = l

	s.logger.Infof("server started successfully, listening on address: %s", l.Addr().String())

	go s.acceptTunnelConn(l)

	<-s.ctx.Done()
}

func (s *DnsTransport) acceptTunnelConn(listener net.Listener) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			s.logger.Debugf("waiting for accept incoming tunnel connection on %s", listener.Addr().String())
			conn, err := listener.Accept()
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				s.logger.Errorf("failed to accept connection: %v", err)
				continue
			}

			select {
			case s.tunnelChannel <- conn:
			default: // The channel is full, do nothing
				s.logger.Warnf("tunnel listener channel is full, discarding DNS connection from %s", conn.RemoteAddr().String())
				conn.Close()
			}
		}
	}
}

func (s *DnsTransport) parsePortMappings() {
	for _, portMapping := range s.config.Ports {
		parts := strings.Split(portMapping, "=")
		var localAddr, remoteAddr string

		if len(parts) == 1 {
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = localPortOrRange

			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Fatalf("invalid port range format: %s", localPortOrRange)
				}

				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Fatalf("invalid start port in range: %s", rangeParts[0])
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Fatalf("invalid end port in range: %s", rangeParts[1])
				}

				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					go s.startListeners(localAddr, strconv.Itoa(port))
					time.Sleep(1 * time.Millisecond)
				}
				continue
			} else {
				port, err := strconv.Atoi(localPortOrRange)
				if err != nil || port < 1 || port > 65535 {
					s.logger.Fatalf("invalid port format: %s", localPortOrRange)
				}
				localAddr = fmt.Sprintf(":%d", port)
			}
		} else if len(parts) == 2 {
			localPortOrRange := strings.TrimSpace(parts[0])
			remoteAddr = strings.TrimSpace(parts[1])

			if strings.Contains(localPortOrRange, "-") {
				rangeParts := strings.Split(localPortOrRange, "-")
				if len(rangeParts) != 2 {
					s.logger.Fatalf("invalid port range format: %s", localPortOrRange)
				}

				startPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
				if err != nil || startPort < 1 || startPort > 65535 {
					s.logger.Fatalf("invalid start port in range: %s", rangeParts[0])
				}

				endPort, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
				if err != nil || endPort < 1 || endPort > 65535 || endPort < startPort {
					s.logger.Fatalf("invalid end port in range: %s", rangeParts[1])
				}

				for port := startPort; port <= endPort; port++ {
					localAddr = fmt.Sprintf(":%d", port)
					go s.startListeners(localAddr, remoteAddr)
					time.Sleep(1 * time.Millisecond)
				}
				continue
			} else {
				port, err := strconv.Atoi(localPortOrRange)
				if err == nil && port >= 1 && port <= 65535 {
					localAddr = fmt.Sprintf(":%d", port)
				} else {
					localAddr = localPortOrRange
				}
			}
		} else {
			s.logger.Fatalf("invalid port mapping format: %s", portMapping)
		}
		go s.startListeners(localAddr, remoteAddr)
	}
}

func (s *DnsTransport) startListeners(localAddr, remoteAddr string) {
	go s.localListener(localAddr, remoteAddr)

	if s.config.AcceptUDP {
		// Just print a warning, UDP forwarding is similar to TCP but omitted for brevity here
		s.logger.Warn("UDP forwarding is enabled but not fully implemented in this example DNS transport")
	}

	s.logger.Debugf("Started listening on %s, forwarding to %s", localAddr, remoteAddr)
}

func (s *DnsTransport) localListener(localAddr string, remoteAddr string) {
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		s.logger.Fatalf("failed to listen on %s: %v", localAddr, err)
		return
	}
	defer listener.Close()

	s.logger.Infof("listener started successfully, listening on address: %s", listener.Addr().String())

	go s.acceptLocalConn(listener, remoteAddr)
	<-s.ctx.Done()
}

func (s *DnsTransport) acceptLocalConn(listener net.Listener, remoteAddr string) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				conn.Close()
				continue
			}

			if !s.config.Nodelay {
				if err := tcpConn.SetNoDelay(s.config.Nodelay); err != nil {
					s.logger.Warnf("failed to set TCP_NODELAY: %v", err)
				}
			}

			select {
			case s.localChannel <- LocalTCPConn{conn: conn, remoteAddr: remoteAddr, timeCreated: time.Now().UnixMilli()}:
				select {
				case s.reqNewConnChan <- struct{}{}:
				default:
				}
			default:
				conn.Close()
			}
		}
	}
}

func (s *DnsTransport) handleLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case localConn := <-s.localChannel:
		loop:
			for {
				if time.Now().UnixMilli()-localConn.timeCreated > 3000 {
					localConn.conn.Close()
					break loop
				}

				select {
				case <-s.ctx.Done():
					return
				case tunnelConn := <-s.tunnelChannel:
					if err := utils.SendBinaryTransportString(tunnelConn, localConn.remoteAddr, utils.SG_TCP); err != nil {
						tunnelConn.Close()
						continue loop
					}

					go handlers.TCPConnectionHandler(s.ctx, s.config.ProxyProtocol, localConn.conn, tunnelConn, s.logger, s.usageMonitor, localConn.conn.LocalAddr().(*net.TCPAddr).Port, s.config.Sniffer)
					break loop
				}
			}
		}
	}
}
