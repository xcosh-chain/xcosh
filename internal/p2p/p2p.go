package p2p

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
	
)

const (
	defaultDialTimeout = 15 * time.Second
	discmixTimeout     = 5 * time.Second

	defaultMaxPendingPeers = 50
	defaultDialRatio       = 3

	inboundThrottleTime = 30 * time.Second
	frameReadTimeout    = 30 * time.Second
	frameWriteTimeout   = 20 * time.Second
)

var (
	errServerStopped       = errors.New("server stopped")
	errEncHandshakeError   = errors.New("rlpx enc error")
	errProtoHandshakeError = errors.New("rlpx proto error")
)

type P2PConfig struct {
	PrivateKey       *ecdsa.PrivateKey `toml:"-"`
	MaxPeers         int
	MaxPendingPeers  int `toml:",omitempty"`
	DialRatio        int `toml:",omitempty"`
	NoDiscovery      bool
	DiscoveryV4      bool `toml:",omitempty"`
	DiscoveryV5      bool `toml:",omitempty"`
	Name             string `toml:"-"`
	BootstrapNodes   []*enode.Node
	BootstrapNodesV5 []*enode.Node `toml:",omitempty"`
	StaticNodes      []*enode.Node
	TrustedNodes     []*enode.Node
	NetRestrict      *netutil.Netlist `toml:",omitempty"`
	NodeDatabase     string `toml:",omitempty"`
	Protocols        []Protocol `toml:"-" json:"-"`
	ListenAddr       string
	DiscAddr         string
	NAT              nat.Interface `toml:",omitempty"`
	Dialer           NodeDialer `toml:"-"`
	NoDial           bool `toml:",omitempty"`
	EnableMsgEvents  bool
	Logger           log.Logger `toml:",omitempty"`
	clock            mclock.Clock
}

type P2PServer struct {
	P2PConfig

	newTransport func(net.Conn, *ecdsa.PublicKey) transport
	newPeerHook  func(*Peer)
	listenFunc   func(network, addr string) (net.Listener, error)

	lock    sync.Mutex
	running bool

	listener     net.Listener
	ourHandshake *protoHandshake
	loopWG       sync.WaitGroup
	peerFeed     event.Feed
	log          log.Logger

	nodedb    *enode.DB
	localnode *enode.LocalNode
	discv4    *discover.UDPv4
	discv5    *discover.UDPv5
	discmix   *enode.FairMix
	dialsched *dialScheduler

	portMappingRegister chan *portMapping

	quit                    chan struct{}
	addtrusted              chan *enode.Node
	removetrusted           chan *enode.Node
	peerOp                  chan peerOpFunc
	peerOpDone              chan struct{}
	delpeer                 chan peerDrop
	checkpointPostHandshake chan *conn
	checkpointAddPeer       chan *conn

	inboundHistory expHeap
}

type peerOpFunc func(map[enode.ID]*Peer)

type peerDrop struct {
	*Peer
	err       error
	requested bool
}

type connFlag int32

const (
	dynDialedConn connFlag = 1 << iota
	staticDialedConn
	inboundConn
	trustedConn
)

type conn struct {
	fd net.Conn
	transport
	node  *enode.Node
	flags connFlag
	cont  chan error
	caps  []Cap
	name  string
}

type transport interface {
	doEncHandshake(prv *ecdsa.PrivateKey) (*ecdsa.PublicKey, error)
	doProtoHandshake(our *protoHandshake) (*protoHandshake, error)
	MsgReadWriter
	close(err error)
}

func (c *conn) String() string {
	s := c.flags.String()
	if (c.node.ID() != enode.ID{}) {
		s += " " + c.node.ID().String()
	}
	s += " " + c.fd.RemoteAddr().String()
	return s
}

func (f connFlag) String() string {
	s := ""
	if f&trustedConn != 0 {
		s += "-trusted"
	}
	if f&dynDialedConn != 0 {
		s += "-dyndial"
	}
	if f&staticDialedConn != 0 {
		s += "-staticdial"
	}
	if f&inboundConn != 0 {
		s += "-inbound"
	}
	if s != "" {
		s = s[1:]
	}
	return s
}

func (c *conn) is(f connFlag) bool {
	flags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
	return flags&f != 0
}

func (c *conn) set(f connFlag, val bool) {
	for {
		oldFlags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
		flags := oldFlags
		if val {
			flags |= f
		} else {
			flags &= ^f
		}
		if atomic.CompareAndSwapInt32((*int32)(&c.flags), int32(oldFlags), int32(flags)) {
			return
		}
	}
}

func (srv *P2PServer) LocalNode() *enode.LocalNode {
	return srv.localnode
}

func (srv *P2PServer) Peers() []*Peer {
	var ps []*Peer
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		for _, p := range peers {
			ps = append(ps, p)
		}
	})
	return ps
}

func (srv *P2PServer) PeerCount() int {
	var count int
	srv.doPeerOp(func(ps map[enode.ID]*Peer) {
		count = len(ps)
	})
	return count
}

func (srv *P2PServer) AddPeer(node *enode.Node) {
	srv.dialsched.addStatic(node)
}

func (srv *P2PServer) RemovePeer(node *enode.Node) {
	var (
		ch  chan *PeerEvent
		sub event.Subscription
	)
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		srv.dialsched.removeStatic(node)
		if peer := peers[node.ID()]; peer != nil {
			ch = make(chan *PeerEvent, 1)
			sub = srv.peerFeed.Subscribe(ch)
			peer.Disconnect(DiscRequested)
		}
	})
	if ch != nil {
		defer sub.Unsubscribe()
		for ev := range ch {
			if ev.Peer == node.ID() && ev.Type == PeerEventTypeDrop {
				return
			}
		}
	}
}

func (srv *P2PServer) AddTrustedPeer(node *enode.Node) {
	select {
	case srv.addtrusted <- node:
	case <-srv.quit:
	}
}

func (srv *P2PServer) RemoveTrustedPeer(node *enode.Node) {
	select {
	case srv.removetrusted <- node:
	case <-srv.quit:
	}
}

func (srv *P2PServer) SubscribeEvents(ch chan *PeerEvent) event.Subscription {
	return srv.peerFeed.Subscribe(ch)
}

func (srv *P2PServer) Self() *enode.Node {
	srv.lock.Lock()
	ln := srv.localnode
	srv.lock.Unlock()

	if ln == nil {
		return enode.NewV4(&srv.PrivateKey.PublicKey, net.ParseIP("0.0.0.0"), 0, 0)
	}
	return ln.Node()
}

func (srv *P2PServer) DiscoveryV4() *discover.UDPv4 {
	return srv.discv4
}

func (srv *P2PServer) DiscoveryV5() *discover.UDPv5 {
	return srv.discv5
}

func (srv *P2PServer) Stop() {
	srv.lock.Lock()
	if !srv.running {
		srv.lock.Unlock()
		return
	}
	srv.running = false
	if srv.listener != nil {
		srv.listener.Close()
	}
	close(srv.quit)
	srv.lock.Unlock()
	srv.loopWG.Wait()
}

type sharedUDPConn struct {
	*net.UDPConn
	unhandled chan discover.ReadPacket
}

func (s *sharedUDPConn) ReadFromUDPAddrPort(b []byte) (n int, addr netip.AddrPort, err error) {
	packet, ok := <-s.unhandled
	if !ok {
		return 0, netip.AddrPort{}, errors.New("connection was closed")
	}
	l := len(packet.Data)
	if l > len(b) {
		l = len(b)
	}
	copy(b[:l], packet.Data[:l])
	return l, packet.Addr, nil
}

func (s *sharedUDPConn) Close() error {
	return nil
}

func (srv *P2PServer) Start() (err error) {
	srv.lock.Lock()
	defer srv.lock.Unlock()
	if srv.running {
		return errors.New("server already running")
	}
	srv.running = true
	srv.log = srv.Logger
	if srv.log == nil {
		srv.log = log.Root()
	}
	if srv.clock == nil {
		srv.clock = mclock.System{}
	}
	if srv.NoDial && srv.ListenAddr == "" {
		srv.log.Warn("P2P server will be useless, neither dialing nor listening")
	}

	if srv.PrivateKey == nil {
		return errors.New("Server.PrivateKey must be set to a non-nil key")
	}
	if srv.newTransport == nil {
		srv.newTransport = newRLPX
	}
	if srv.listenFunc == nil {
		srv.listenFunc = net.Listen
	}
	srv.quit = make(chan struct{})
	srv.delpeer = make(chan peerDrop)
	srv.checkpointPostHandshake = make(chan *conn)
	srv.checkpointAddPeer = make(chan *conn)
	srv.addtrusted = make(chan *enode.Node)
	srv.removetrusted = make(chan *enode.Node)
	srv.peerOp = make(chan peerOpFunc)
	srv.peerOpDone = make(chan struct{})

	if err := srv.setupLocalNode(); err != nil {
		return err
	}
	srv.setupPortMapping()

	if srv.ListenAddr != "" {
		if err := srv.setupListening(); err != nil {
			return err
		}
	}
	if err := srv.setupDiscovery(); err != nil {
		return err
	}
	srv.setupDialScheduler()

	srv.loopWG.Add(1)
	go srv.run()
	return nil
}

func (srv *P2PServer) setupLocalNode() error {
	pubkey := crypto.FromECDSAPub(&srv.PrivateKey.PublicKey)
	srv.ourHandshake = &protoHandshake{Version: baseProtocolVersion, Name: srv.Name, ID: pubkey[1:]}
	for _, p := range srv.Protocols {
		srv.ourHandshake.Caps = append(srv.ourHandshake.Caps, p.cap())
	}
	slices.SortFunc(srv.ourHandshake.Caps, Cap.Cmp)

	db, err := enode.OpenDB(srv.NodeDatabase)
	if err != nil {
		return err
	}
	srv.nodedb = db
	srv.localnode = enode.NewLocalNode(db, srv.PrivateKey)
	srv.localnode.SetFallbackIP(net.IP{127, 0, 0, 1})
	for _, p := range srv.Protocols {
		for _, e := range p.Attributes {
			srv.localnode.Set(e)
		}
	}
	return nil
}

func (srv *P2PServer) setupDiscovery() error {
	srv.discmix = enode.NewFairMix(discmixTimeout)

	if srv.NoDiscovery {
		return nil
	}
	conn, err := srv.setupUDPListening()
	if err != nil {
		return err
	}

	var (
		sconn     discover.UDPConn = conn
		unhandled chan discover.ReadPacket
	)
	if srv.DiscoveryV4 && srv.DiscoveryV5 {
		unhandled = make(chan discover.ReadPacket, 100)
		sconn = &sharedUDPConn{conn, unhandled}
	}

	if srv.DiscoveryV4 {
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodes,
			Unhandled:   unhandled,
			Log:         srv.log,
		}
		ntab, err := discover.ListenV4(conn, srv.localnode, cfg)
		if err != nil {
			return err
		}
		srv.discv4 = ntab
		srv.discmix.AddSource(ntab.RandomNodes())
	}
	if srv.DiscoveryV5 {
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodesV5,
			Log:         srv.log,
		}
		srv.discv5, err = discover.ListenV5(sconn, srv.localnode, cfg)
		if err != nil {
			return err
		}
	}

	added := make(map[string]bool)
	for _, proto := range srv.Protocols {
		if proto.DialCandidates != nil && !added[proto.Name] {
			srv.discmix.AddSource(proto.DialCandidates)
			added[proto.Name] = true
		}
	}
	return nil
}

func (srv *P2PServer) setupDialScheduler() {
	config := dialConfig{
		self:           srv.localnode.ID(),
		maxDialPeers:   srv.maxDialedConns(),
		maxActiveDials: srv.MaxPendingPeers,
		log:            srv.Logger,
		netRestrict:    srv.NetRestrict,
		dialer:         srv.Dialer,
		clock:          srv.clock,
	}
	if srv.discv4 != nil {
		config.resolver = srv.discv4
	}
	if config.dialer == nil {
		config.dialer = tcpDialer{&net.Dialer{Timeout: defaultDialTimeout}}
	}
	srv.dialsched = newDialScheduler(config, srv.discmix, srv.SetupConn)
	for _, n := range srv.StaticNodes {
		srv.dialsched.addStatic(n)
	}
}

func (srv *P2PServer) maxInboundConns() int {
	return srv.MaxPeers - srv.maxDialedConns()
}

func (srv *P2PServer) maxDialedConns() (limit int) {
	if srv.NoDial || srv.MaxPeers == 0 {
		return 0
	}
	if srv.DialRatio == 0 {
		limit = srv.MaxPeers / defaultDialRatio
	} else {
		limit = srv.MaxPeers / srv.DialRatio
	}
	if limit == 0 {
		limit = 1
	}
	return limit
}

func (srv *P2PServer) setupListening() error {
	listener, err := srv.listenFunc("tcp", srv.ListenAddr)
	if err != nil {
		return err
	}
	srv.listener = listener
	srv.ListenAddr = listener.Addr().String()

	tcp, isTCP := listener.Addr().(*net.TCPAddr)
	if isTCP {
		srv.localnode.Set(enr.TCP(tcp.Port))
		if !tcp.IP.IsLoopback() && !tcp.IP.IsPrivate() {
			srv.portMappingRegister <- &portMapping{
				protocol: "TCP",
				name:     "xcosh p2p",
				port:     tcp.Port,
			}
		}
	}

	srv.loopWG.Add(1)
	go srv.listenLoop()
	return nil
}

func (srv *P2PServer) setupUDPListening() (*net.UDPConn, error) {
	listenAddr := srv.ListenAddr
	if srv.DiscAddr != "" {
		listenAddr = srv.DiscAddr
	}
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)
	srv.localnode.SetFallbackUDP(laddr.Port)
	srv.log.Debug("UDP listener up", "addr", laddr)
	if !laddr.IP.IsLoopback() && !laddr.IP.IsPrivate() {
		srv.portMappingRegister <- &portMapping{
			protocol: "UDP",
			name:     "xcosh peer discovery",
			port:     laddr.Port,
		}
	}

	return conn, nil
}

func (srv *P2PServer) doPeerOp(fn peerOpFunc) {
	select {
	case srv.peerOp <- fn:
		<-srv.peerOpDone
	case <-srv.quit:
	}
}

func (srv *P2PServer) run() {
	srv.log.Info("Started xcosh P2P networking", "self", srv.localnode.Node().URLv4())
	defer srv.loopWG.Done()
	defer srv.nodedb.Close()
	defer srv.discmix.Close()
	defer srv.dialsched.stop()

	var (
		peers        = make(map[enode.ID]*Peer)
		inboundCount = 0
		trusted      = make(map[enode.ID]bool, len(srv.TrustedNodes))
	)
	for _, n := range srv.TrustedNodes {
		trusted[n.ID()] = true
	}

running:
	for {
		select {
		case <-srv.quit:
			break running

		case n := <-srv.addtrusted:
			trusted[n.ID()] = true
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, true)
			}

		case n := <-srv.removetrusted:
			delete(trusted, n.ID())
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, false)
			}

		case op := <-srv.peerOp:
			op(peers)
			srv.peerOpDone <- struct{}{}

		case c := <-srv.checkpointPostHandshake:
			if trusted[c.node.ID()] {
				c.flags |= trustedConn
			}
			c.cont <- srv.postHandshakeChecks(peers, inboundCount, c)

		case c := <-srv.checkpointAddPeer:
			err := srv.addPeerChecks(peers, inboundCount, c)
			if err == nil {
				p := srv.launchPeer(c)
				peers[c.node.ID()] = p
				srv.dialsched.peerAdded(c)
				if p.Inbound() {
					inboundCount++
					serveSuccessMeter.Mark(1)
					activeInboundPeerGauge.Inc(1)
				} else {
					dialSuccessMeter.Mark(1)
					activeOutboundPeerGauge.Inc(1)
				}
				activePeerGauge.Inc(1)
			}
			c.cont <- err

		case pd := <-srv.delpeer:
			delete(peers, pd.ID())
			srv.dialsched.peerRemoved(pd.rw)
			if pd.Inbound() {
				inboundCount--
				activeInboundPeerGauge.Dec(1)
			} else {
				activeOutboundPeerGauge.Dec(1)
			}
			activePeerGauge.Dec(1)
		}
	}

	if srv.discv4 != nil {
		srv.discv4.Close()
	}
	if srv.discv5 != nil {
		srv.discv5.Close()
	}
	for _, p := range peers {
		p.Disconnect(DiscQuitting)
	}
	for len(peers) > 0 {
		p := <-srv.delpeer
		delete(peers, p.ID())
	}
}

func (srv *P2PServer) postHandshakeChecks(peers map[enode.ID]*Peer, inboundCount int, c *conn) error {
	switch {
	case !c.is(trustedConn) && len(peers) >= srv.MaxPeers:
		return DiscTooManyPeers
	case !c.is(trustedConn) && c.is(inboundConn) && inboundCount >= srv.maxInboundConns():
		return DiscTooManyPeers
	case peers[c.node.ID()] != nil:
		return DiscAlreadyConnected
	case c.node.ID() == srv.localnode.ID():
		return DiscSelf
	default:
		return nil
	}
}

func (srv *P2PServer) addPeerChecks(peers map[enode.ID]*Peer, inboundCount int, c *conn) error {
	if len(srv.Protocols) > 0 && countMatchingProtocols(srv.Protocols, c.caps) == 0 {
		return DiscUselessPeer
	}
	return srv.postHandshakeChecks(peers, inboundCount, c)
}

func (srv *P2PServer) listenLoop() {
	tokens := defaultMaxPendingPeers
	if srv.MaxPendingPeers > 0 {
		tokens = srv.MaxPendingPeers
	}
	slots := make(chan struct{}, tokens)
	for i := 0; i < tokens; i++ {
		slots <- struct{}{}
	}

	defer srv.loopWG.Done()
	defer func() {
		for i := 0; i < cap(slots); i++ {
			<-slots
		}
	}()

	for {
		<-slots
		fd, err := srv.listener.Accept()
		if err != nil {
			slots <- struct{}{}
			return
		}

		remoteIP := netutil.AddrAddr(fd.RemoteAddr())
		if err := srv.checkInboundConn(remoteIP); err != nil {
			fd.Close()
			slots <- struct{}{}
			continue
		}
		if remoteIP.IsValid() {
			fd = newMeteredConn(fd)
			serveMeter.Mark(1)
		}
		go func() {
			srv.SetupConn(fd, inboundConn, nil)
			slots <- struct{}{}
		}()
	}
}

func (srv *P2PServer) checkInboundConn(remoteIP netip.Addr) error {
	if !remoteIP.IsValid() {
		return nil
	}
	if srv.NetRestrict != nil && !srv.NetRestrict.ContainsAddr(remoteIP) {
		return errors.New("not in netrestrict list")
	}
	now := srv.clock.Now()
	srv.inboundHistory.expire(now, nil)
	if !netutil.AddrIsLAN(remoteIP) && srv.inboundHistory.contains(remoteIP.String()) {
		return errors.New("too many attempts")
	}
	srv.inboundHistory.add(remoteIP.String(), now.Add(inboundThrottleTime))
	return nil
}

func (srv *P2PServer) SetupConn(fd net.Conn, flags connFlag, dialDest *enode.Node) error {
	c := &conn{fd: fd, flags: flags, cont: make(chan error)}
	if dialDest == nil {
		c.transport = srv.newTransport(fd, nil)
	} else {
		c.transport = srv.newTransport(fd, dialDest.Pubkey())
	}

	err := srv.setupConn(c, dialDest)
	if err != nil {
		if !c.is(inboundConn) {
			markDialError(err)
		}
		c.close(err)
	}
	return err
}

func (srv *P2PServer) setupConn(c *conn, dialDest *enode.Node) error {
	srv.lock.Lock()
	running := srv.running
	srv.lock.Unlock()
	if !running {
		return errServerStopped
	}

	if dialDest != nil {
		dialPubkey := new(ecdsa.PublicKey)
		if err := dialDest.Load((*enode.Secp256k1)(dialPubkey)); err != nil {
			return fmt.Errorf("%w: dial destination doesn't have a secp256k1 public key", errEncHandshakeError)
		}
	}

	remotePubkey, err := c.doEncHandshake(srv.PrivateKey)
	if err != nil {
		return fmt.Errorf("%w: %v", errEncHandshakeError, err)
	}
	if dialDest != nil {
		c.node = dialDest
	} else {
		c.node = nodeFromConn(remotePubkey, c.fd)
	}
	err = srv.checkpoint(c, srv.checkpointPostHandshake)
	if err != nil {
		return err
	}

	phs, err := c.doProtoHandshake(srv.ourHandshake)
	if err != nil {
		return fmt.Errorf("%w: %v", errProtoHandshakeError, err)
	}
	if id := c.node.ID(); !bytes.Equal(crypto.Keccak256(phs.ID), id[:]) {
		return DiscUnexpectedIdentity
	}
	c.caps, c.name = phs.Caps, phs.Name
	err = srv.checkpoint(c, srv.checkpointAddPeer)
	if err != nil {
		return err
	}

	return nil
}

func nodeFromConn(pubkey *ecdsa.PublicKey, conn net.Conn) *enode.Node {
	var ip net.IP
	var port int
	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		ip = tcp.IP
		port = tcp.Port
	}
	return enode.NewV4(pubkey, ip, port, port)
}

func (srv *P2PServer) checkpoint(c *conn, stage chan<- *conn) error {
	select {
	case stage <- c:
	case <-srv.quit:
		return errServerStopped
	}
	return <-c.cont
}

func (srv *P2PServer) launchPeer(c *conn) *Peer {
	p := newPeer(srv.log, c, srv.Protocols)
	if srv.EnableMsgEvents {
		p.events = &srv.peerFeed
	}
	go srv.runPeer(p)
	return p
}

func (srv *P2PServer) runPeer(p *Peer) {
	if srv.newPeerHook != nil {
		srv.newPeerHook(p)
	}
	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeAdd,
		Peer:          p.ID(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})

	remoteRequested, err := p.run()

	srv.delpeer <- peerDrop{p, err, remoteRequested}

	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeDrop,
		Peer:          p.ID(),
		Error:         err.Error(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})
}

type NodeInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Enode  string `json:"enode"`
	ENR    string `json:"enr"`
	IP     string `json:"ip"`
	Ports  struct {
		Discovery int `json:"discovery"`
		Listener  int `json:"listener"`
	} `json:"ports"`
	ListenAddr string                 `json:"listenAddr"`
	Protocols  map[string]interface{} `json:"protocols"`
}

func (srv *P2PServer) NodeInfo() *NodeInfo {
	node := srv.Self()
	info := &NodeInfo{
		Name:       srv.Name,
		Enode:      node.URLv4(),
		ID:         node.ID().String(),
		IP:         node.IPAddr().String(),
		ListenAddr: srv.ListenAddr,
	}
	info.Ports.Discovery = node.UDP()
	info.Ports.Listener = node.TCP()
	info.ENR = node.String()
	return info
}
