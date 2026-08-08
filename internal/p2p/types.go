package p2p

import (
	"errors"
	"net"
	"net/netip"
	"time"
)

// Menyesuaikan dengan standar Dilithium3
type DilithiumPublicKey [1952]byte
type DilithiumPrivateKey [4016]byte

type Node struct{}
func (n *Node) ID() NodeID { return NodeID{} }
func (n *Node) Pubkey() *DilithiumPublicKey { return &DilithiumPublicKey{} }
func (n *Node) URLv4() string { return "" }
func (n *Node) IPAddr() net.IP { return net.IP{} }
func (n *Node) UDP() int { return 0 }
func (n *Node) TCP() int { return 0 }
func (n *Node) String() string { return "" }

func NewNodeV4(pubkey *DilithiumPublicKey, ip net.IP, udpPort, tcpPort int) *Node {
	return &Node{}
}

type NodeID [32]byte
func (id NodeID) String() string { return "" }

type Netlist struct{}
func (n *Netlist) ContainsAddr(addr netip.Addr) bool { return true }

type Protocol struct {
	Name           string
	Version        uint
	Length         uint
	Run            func(p *Peer, rw MsgReadWriter) error
	NodeInfo       func() interface{}
	PeerInfo       func(id NodeID) interface{}
	DialCandidates interface{}
	Attributes     []ENREntry
}

func (p Protocol) cap() Cap { return Cap{} }

type Cap struct {
	Name    string
	Version uint
}

type NATInterface interface{}
type NodeDialer interface{}
type Logger interface {
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
}

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}
func (SystemClock) Now() time.Time { return time.Now() }

type ENREntry interface{}

type UDPv4 struct{}
func (u *UDPv4) Close() {}
func (u *UDPv4) RandomNodes() interface{} { return nil }

type UDPv5 struct{}
func (u *UDPv5) Close() {}

type FairMix struct{}
func NewFairMix(d time.Duration) *FairMix { return &FairMix{} }
func (f *FairMix) AddSource(any interface{}) {}
func (f *FairMix) Close() {}

type dialScheduler struct{}
func newDialScheduler(cfg dialConfig, mix *FairMix, connect func(net.Conn, connFlag, *Node) error) *dialScheduler {
	return &dialScheduler{}
}
func (ds *dialScheduler) addStatic(n *Node) {}
func (ds *dialScheduler) removeStatic(n *Node) {}
func (ds *dialScheduler) peerAdded(c *conn) {}
func (ds *dialScheduler) peerRemoved(rw MsgReadWriter) {}
func (ds *dialScheduler) stop() {}

type dialConfig struct {
	self           NodeID
	maxDialPeers   int
	maxActiveDials int
	log            Logger
	netRestrict    *Netlist
	dialer         NodeDialer
	clock          Clock
	resolver       interface{}
}

type UDPConn interface{}
type ReadPacket struct {
	Data []byte
	Addr netip.AddrPort
}

type DiscoveryConfig struct {
	PrivateKey  *DilithiumPrivateKey
	NetRestrict *Netlist
	Bootnodes   []*Node
	Unhandled   chan ReadPacket
	Log         Logger
}

type NodeDB struct{}
func OpenNodeDB(path string) (*NodeDB, error) { return &NodeDB{}, nil }
func (db *NodeDB) Close() error { return nil }

type LocalNode struct{}
func NewLocalNode(db *NodeDB, prv *DilithiumPrivateKey) *LocalNode { return &LocalNode{} }
func (ln *LocalNode) SetFallbackIP(ip net.IP) {}
func (ln *LocalNode) SetFallbackUDP(port int) {}
func (ln *LocalNode) Set(entry ENREntry) {}
func (ln *LocalNode) Node() *Node { return &Node{} }
func (ln *LocalNode) ID() NodeID { return NodeID{} }

func ListenV4(conn *net.UDPConn, ln *LocalNode, cfg DiscoveryConfig) (*UDPv4, error) {
	return &UDPv4{}, nil
}

func ListenV5(conn UDPConn, ln *LocalNode, cfg DiscoveryConfig) (*UDPv5, error) {
	return &UDPv5{}, nil
}

type protoHandshake struct {
	Version uint
	Name    string
	ID      []byte
	Caps    []Cap
}

const baseProtocolVersion = 1

func SortCaps(caps []Cap) {}
func RootLogger() Logger { return nil }
func AddrAddr(addr net.Addr) netip.Addr { return netip.Addr{} }
func AddrIsLAN(addr netip.Addr) bool { return false }
func newMeteredConn(fd net.Conn) net.Conn { return fd }
func serveMeterMark() {}
func serveSuccessMeterMark() {}
func dialSuccessMeterMark() {}
func markDialError(err error) {}
func activeInboundPeerDecInc(inc bool) {}
func activeOutboundPeerDecInc(inc bool) {}
func activePeerDecInc(inc bool) {}

type tcpDialer struct{ *net.Dialer }

var (
	DiscTooManyPeers       = errors.New("too many peers")
	DiscAlreadyConnected   = errors.New("already connected")
	DiscSelf               = errors.New("self connection")
	DiscUselessPeer        = errors.New("useless peer")
	DiscUnexpectedIdentity = errors.New("unexpected identity")
	DiscQuitting           = errors.New("quitting")
	DiscRequested          = errors.New("requested")
)

type Peer struct {
	rw *conn
}
func newPeer(log Logger, c *conn, protos []Protocol) *Peer { return &Peer{rw: c} }
func (p *Peer) ID() NodeID { return NodeID{} }
func (p *Peer) RemoteAddr() net.Addr { return nil }
func (p *Peer) LocalAddr() net.Addr { return nil }
func (p *Peer) Inbound() bool { return false }
func (p *Peer) Disconnect(reason error) {}
func (p *Peer) run() (bool, error) { return false, nil }

type MsgReadWriter interface{}

func newRLPX(conn net.Conn, pubkey *DilithiumPublicKey) transport { return nil }
func countMatchingProtocols(protos []Protocol, caps []Cap) int { return 1 }
func Keccak256(data ...[]byte) [32]byte { return [32]byte{} }

type EventFeed struct{}
func (ef *EventFeed) Subscribe(ch chan *PeerEvent) Subscription { return nil }
func (ef *EventFeed) Send(ev interface{}) {}

type Subscription interface {
	Unsubscribe()
}

type PeerEventType int
const (
	PeerEventTypeAdd PeerEventType = iota
	PeerEventTypeDrop
)

type PeerEvent struct {
	Type          PeerEventType
	Peer          NodeID
	Error         string
	RemoteAddress string
	LocalAddress  string
}

type expHeap struct{}
func (eh *expHeap) expire(now time.Time, limit interface{}) {}
func (eh *expHeap) contains(s string) bool { return false }
func (eh *expHeap) add(s string, t time.Time) {}

type portMapping struct {
	protocol string
	name     string
	port     int
}

func (srv *P2PServer) setupPortMapping() {}
func ENRTCP(port int) ENREntry { return nil }
