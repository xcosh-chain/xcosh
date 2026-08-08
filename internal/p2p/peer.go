package p2p

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

var (
	ErrShuttingDown = errors.New("shutting down")
)

const (
	baseProtocolVersion    = 5
	baseProtocolLength     = uint64(16)
	baseProtocolMaxMsgSize = 2 * 1024

	snappyProtocolVersion = 5

	pingInterval = 15 * time.Second
)

const (
	// devp2p message codes
	handshakeMsg = 0x00
	discMsg      = 0x01
	pingMsg      = 0x02
	pongMsg      = 0x03
)

// protoHandshake adalah struktur RLP untuk handshake protokol.
type protoHandshake struct {
	Version    uint64
	Name       string
	Caps       []Cap
	ListenPort uint64
	ID         []byte // secp256k1 public key

	Rest []RawValue `rlp:"tail"`
}

// PeerEventType adalah tipe event peer yang dipancarkan oleh server p2p.
type PeerEventType string

const (
	PeerEventTypeAdd     PeerEventType = "add"
	PeerEventTypeDrop    PeerEventType = "drop"
	PeerEventTypeMsgSend PeerEventType = "msgsend"
	PeerEventTypeMsgRecv PeerEventType = "msgrecv"
)

// PeerEvent merepresentasikan event saat peer ditambah/dihapus atau pesan dikirim/diterima.
type PeerEvent struct {
	Type          PeerEventType `json:"type"`
	Peer          NodeID        `json:"peer"`
	Error         string        `json:"error,omitempty"`
	Protocol      string        `json:"protocol,omitempty"`
	MsgCode       *uint64       `json:"msg_code,omitempty"`
	MsgSize       *uint32       `json:"msg_size,omitempty"`
	LocalAddress  string        `json:"local,omitempty"`
	RemoteAddress string        `json:"remote,omitempty"`
}

// Peer merepresentasikan node remote yang terhubung.
type Peer struct {
	rw       *conn
	running  map[string]*protoRW
	log      Logger
	created  time.Time

	wg       sync.WaitGroup
	protoErr chan error
	closed   chan struct{}
	pingRecv chan struct{}
	disc     chan DiscReason

	events   interface{} // Disesuaikan jika event feed lokal belum ada
	testPipe *MsgPipeRW  // untuk pengujian
}

// NewPeer mengembalikan peer untuk keperluan testing.
func NewPeer(id NodeID, name string, caps []Cap) *Peer {
	protos := make([]Protocol, len(caps))
	for i, cap := range caps {
		protos[i].Name = cap.Name
		protos[i].Version = cap.Version
	}
	pipe, _ := net.Pipe()
	
	// Membuat node dummy untuk testing
	node := &Node{IP: net.ParseIP("127.0.0.1"), TCPPort: 30303, UDPPort: 30303}
	node.id = id

	conn := &conn{fd: pipe, transport: nil, node: node, caps: caps, name: name}
	peer := newPeer(NewLogger(), conn, protos)
	close(peer.closed) // memastikan Disconnect tidak blok
	return peer
}

// NewPeerPipe membuat peer dengan pipe untuk testing.
func NewPeerPipe(id NodeID, name string, caps []Cap, pipe *MsgPipeRW) *Peer {
	p := NewPeer(id, name, caps)
	p.testPipe = pipe
	return p
}

// ID mengembalikan public key node.
func (p *Peer) ID() NodeID {
	return p.rw.node.ID()
}

// Node mengembalikan descriptor node peer.
func (p *Peer) Node() *Node {
	return p.rw.node
}

// Name mengembalikan bentuk singkat dari nama node.
func (p *Peer) Name() string {
	s := p.rw.name
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}

// Fullname mengembalikan nama lengkap node remote.
func (p *Peer) Fullname() string {
	return p.rw.name
}

// Caps mengembalikan kapabilitas (subprotokol yang didukung) dari peer remote.
func (p *Peer) Caps() []Cap {
	return p.rw.caps
}

// RunningCap memeriksa apakah peer aktif menggunakan versi dari protokol tertentu.
func (p *Peer) RunningCap(protocol string, versions []uint) bool {
	if proto, ok := p.running[protocol]; ok {
		for _, ver := range versions {
			if proto.Version == uint(ver) {
				return true
			}
		}
	}
	return false
}

// RemoteAddr mengembalikan alamat remote dari koneksi network.
func (p *Peer) RemoteAddr() net.Addr {
	return p.rw.fd.RemoteAddr()
}

// LocalAddr mengembalikan alamat lokal dari koneksi network.
func (p *Peer) LocalAddr() net.Addr {
	return p.rw.fd.LocalAddr()
}

// Disconnect menghentikan koneksi peer dengan alasan tertentu.
func (p *Peer) Disconnect(reason DiscReason) {
	if p.testPipe != nil {
		p.testPipe.Close()
	}

	select {
	case p.disc <- reason:
	case <-p.closed:
	}
}

// String mengimplementasikan fmt.Stringer.
func (p *Peer) String() string {
	id := p.ID()
	return fmt.Sprintf("Peer %x %v", id[:8], p.RemoteAddr())
}

// Inbound mengembalikan true jika peer adalah koneksi masuk (inbound).
func (p *Peer) Inbound() bool {
	return p.rw.is(inboundConn)
}

func newPeer(log Logger, conn *conn, protocols []Protocol) *Peer {
	protomap := matchProtocols(protocols, conn.caps, conn)
	p := &Peer{
		rw:       conn,
		running:  protomap,
		created:  time.Now(),
		disc:     make(chan DiscReason),
		protoErr: make(chan error, len(protomap)+1),
		closed:   make(chan struct{}),
		pingRecv: make(chan struct{}, 16),
		log:      log,
	}
	return p
}

func (p *Peer) Log() Logger {
	return p.log
}

func (p *Peer) run() (remoteRequested bool, err error) {
	var (
		writeStart = make(chan struct{}, 1)
		writeErr   = make(chan error, 1)
		readErr    = make(chan error, 1)
		reason     DiscReason
	)
	p.wg.Add(2)
	go p.readLoop(readErr)
	go p.pingLoop()

	writeStart <- struct{}{}
	p.startProtocols(writeStart, writeErr)

loop:
	for {
		select {
		case err = <-writeErr:
			if err != nil {
				reason = DiscNetworkError
				break loop
			}
			writeStart <- struct{}{}
		case err = <-readErr:
			if r, ok := err.(DiscReason); ok {
				remoteRequested = true
				reason = r
			} else {
				reason = DiscNetworkError
			}
			break loop
		case err = <-p.protoErr:
			reason = discReasonForError(err)
			break loop
		case err = <-p.disc:
			reason = discReasonForError(err)
			break loop
		}
	}

	close(p.closed)
	p.rw.close(reason)
	p.wg.Wait()
	return remoteRequested, err
}

func (p *Peer) pingLoop() {
	defer p.wg.Done()

	ping := time.NewTimer(pingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ping.C:
			if err := SendItems(p.rw, pingMsg); err != nil {
				p.protoErr <- err
				return
			}
			ping.Reset(pingInterval)

		case <-p.pingRecv:
			SendItems(p.rw, pongMsg)

		case <-p.closed:
			return
		}
	}
}

func (p *Peer) readLoop(errc chan<- error) {
	defer p.wg.Done()
	for {
		msg, err := p.rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		msg.ReceivedAt = time.Now()
		if err = p.handle(msg); err != nil {
			errc <- err
			return
		}
	}
}

func (p *Peer) handle(msg Msg) error {
	switch {
	case msg.Code == pingMsg:
		msg.Discard()
		select {
		case p.pingRecv <- struct{}{}:
		case <-p.closed:
		}
	case msg.Code == discMsg:
		var m struct{ R DiscReason }
		Decode(msg.Payload, &m)
		return m.R
	case msg.Code < baseProtocolLength:
		return msg.Discard()
	default:
		proto, err := p.getProto(msg.Code)
		if err != nil {
			return fmt.Errorf("msg code out of range: %v", msg.Code)
		}
		select {
		case proto.in <- msg:
			return nil
		case <-p.closed:
			return io.EOF
		}
	}
	return nil
}

func countMatchingProtocols(protocols []Protocol, caps []Cap) int {
	n := 0
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				n++
			}
		}
	}
	return n
}

func matchProtocols(protocols []Protocol, caps []Cap, rw MsgReadWriter) map[string]*protoRW {
	slices.SortFunc(caps, Cap.Cmp)
	offset := baseProtocolLength
	result := make(map[string]*protoRW)

outer:
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				if old := result[cap.Name]; old != nil {
					offset -= old.Length
				}
				result[cap.Name] = &protoRW{Protocol: proto, offset: offset, in: make(chan Msg), w: rw}
				offset += proto.Length

				continue outer
			}
		}
	}
	return result
}

func (p *Peer) startProtocols(writeStart <-chan struct{}, writeErr chan<- error) {
	p.wg.Add(len(p.running))
	for _, proto := range p.running {
		proto := proto
		proto.closed = p.closed
		proto.wstart = writeStart
		proto.werr = writeErr
		var rw MsgReadWriter = proto

		p.log.Trace(fmt.Sprintf("Starting protocol %s/%d", proto.Name, proto.Version))
		go func() {
			defer p.wg.Done()
			err := proto.Run(p, rw)
			if err == nil {
				p.log.Trace(fmt.Sprintf("Protocol %s/%d returned", proto.Name, proto.Version))
				err = errProtocolReturned
			} else if !errors.Is(err, io.EOF) {
				p.log.Trace(fmt.Sprintf("Protocol %s/%d failed", proto.Name, proto.Version), "err", err)
			}
			p.protoErr <- err
		}()
	}
}

func (p *Peer) getProto(code uint64) (*protoRW, error) {
	for _, proto := range p.running {
		if code >= proto.offset && code < proto.offset+proto.Length {
			return proto, nil
		}
	}
	return nil, newPeerError(errInvalidMsgCode, "%d", code)
}

type protoRW struct {
	Protocol
	in     chan Msg
	closed <-chan struct{}
	wstart <-chan struct{}
	werr   chan<- error
	offset uint64
	w      MsgWriter
}

func (rw *protoRW) WriteMsg(msg Msg) (err error) {
	if msg.Code >= rw.Length {
		return newPeerError(errInvalidMsgCode, "not handled")
	}
	msg.meterCap = rw.cap()
	msg.meterCode = msg.Code

	msg.Code += rw.offset

	select {
	case <-rw.wstart:
		err = rw.w.WriteMsg(msg)
		rw.werr <- err
	case <-rw.closed:
		err = ErrShuttingDown
	}
	return err
}

func (rw *protoRW) ReadMsg() (Msg, error) {
	select {
	case msg := <-rw.in:
		msg.Code -= rw.offset
		return msg, nil
	case <-rw.closed:
		return Msg{}, io.EOF
	}
}

// PeerInfo merepresentasikan ringkasan informasi peer yang terhubung.
type PeerInfo struct {
	ENR   string   `json:"enr,omitempty"`
	Enode string   `json:"enode"`
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Caps  []string `json:"caps"`
	Network struct {
		LocalAddress  string `json:"localAddress"`
		RemoteAddress string `json:"remoteAddress"`
		Inbound       bool   `json:"inbound"`
		Trusted       bool   `json:"trusted"`
		Static        bool   `json:"static"`
	} `json:"network"`
	Protocols map[string]interface{} `json:"protocols"`
}

// Info mengumpulkan dan mengembalikan metadata peer.
func (p *Peer) Info() *PeerInfo {
	var caps []string
	for _, cap := range p.Caps() {
		caps = append(caps, cap.String())
	}
	
	idStr := fmt.Sprintf("%x", p.ID())
	info := &PeerInfo{
		Enode:     p.Node().URLv4(),
		ID:        idStr,
		Name:      p.Fullname(),
		Caps:      caps,
		Protocols: make(map[string]interface{}, len(p.running)),
	}
	
	info.Network.LocalAddress = p.LocalAddr().String()
	info.Network.RemoteAddress = p.RemoteAddr().String()
	info.Network.Inbound = p.rw.is(inboundConn)
	info.Network.Trusted = p.rw.is(trustedConn)
	info.Network.Static = p.rw.is(staticDialedConn)

	for _, proto := range p.running {
		protoInfo := interface{}("unknown")
		if query := proto.Protocol.PeerInfo; query != nil {
			if metadata := query(p.ID()); metadata != nil {
				protoInfo = metadata
			} else {
				protoInfo = "handshake"
			}
		}
		info.Protocols[proto.Name] = protoInfo
	}
	return info
}
