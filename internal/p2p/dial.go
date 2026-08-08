package p2p

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	mrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dialHistoryExpiration = inboundThrottleTime + 5*time.Second
	dialStatsLogInterval  = 10 * time.Second
	dialStatsPeerLimit    = 3

	initialResolveDelay = 60 * time.Second
	maxResolveDelay     = time.Hour
)

// NodeDialer mendefinisikan interface untuk melakukan dial koneksi ke node.
type NodeDialer interface {
	Dial(context.Context, *Node) (net.Conn, error)
}

type nodeResolver interface {
	Resolve(*Node) *Node
}

// tcpDialer mengimplementasikan NodeDialer menggunakan TCP murni.
type tcpDialer struct {
	d *net.Dialer
}

func (t tcpDialer) Dial(ctx context.Context, dest *Node) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", dest.IP.String(), dest.TCPPort)
	return t.d.DialContext(ctx, "tcp", addr)
}

var (
	errSelf             = errors.New("is self")
	errAlreadyDialing   = errors.New("already dialing")
	errAlreadyConnected = errors.New("already connected")
	errRecentlyDialed   = errors.New("recently dialed")
	errNetRestrict      = errors.New("not contained in netrestrict list")
	errNoPort           = errors.New("node does not provide TCP port")
)

type dialScheduler struct {
	dialConfig
	setupFunc   dialSetupFunc
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	nodesIn     chan *Node
	doneCh      chan *dialTask
	addStaticCh chan *Node
	remStaticCh chan *Node
	addPeerCh   chan *conn
	remPeerCh   chan *conn

	dialing   map[NodeID]*dialTask
	peers     map[NodeID]struct{}
	dialPeers int

	static     map[NodeID]*dialTask
	staticPool []*dialTask

	history      expHeap
	historyTimer *time.Timer

	lastStatsLog     time.Time
	doneSinceLastLog int
}

type dialSetupFunc func(net.Conn, connFlag, *Node) error

type dialConfig struct {
	self           NodeID
	maxDialPeers   int
	maxActiveDials int
	netRestrict    interface{} // Disederhanakan jika netutil belum ada
	resolver       nodeResolver
	dialer         NodeDialer
	rand           *mrand.Rand
}

func (cfg dialConfig) withDefaults() dialConfig {
	if cfg.maxActiveDials == 0 {
		cfg.maxActiveDials = defaultMaxPendingPeers
	}
	if cfg.dialer == nil {
		cfg.dialer = tcpDialer{d: &net.Dialer{Timeout: 5 * time.Second}}
	}
	if cfg.rand == nil {
		seedb := make([]byte, 8)
		crand.Read(seedb)
		seed := int64(binary.BigEndian.Uint64(seedb))
		cfg.rand = mrand.New(mrand.NewSource(seed))
	}
	return cfg
}

func newDialScheduler(config dialConfig, it interface{}, setupFunc dialSetupFunc) *dialScheduler {
	cfg := config.withDefaults()
	d := &dialScheduler{
		dialConfig:  cfg,
		setupFunc:   setupFunc,
		dialing:     make(map[NodeID]*dialTask),
		static:      make(map[NodeID]*dialTask),
		peers:       make(map[NodeID]struct{}),
		doneCh:      make(chan *dialTask),
		nodesIn:     make(chan *Node),
		addStaticCh: make(chan *Node),
		remStaticCh: make(chan *Node),
		addPeerCh:   make(chan *conn),
		remPeerCh:   make(chan *conn),
		historyTimer: time.NewTimer(time.Hour),
	}
	d.lastStatsLog = time.Now()
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.wg.Add(1)
	go d.loop()
	return d
}

func (d *dialScheduler) stop() {
	d.cancel()
	d.historyTimer.Stop()
	d.wg.Wait()
}

func (d *dialScheduler) addStatic(n *Node) {
	select {
	case d.addStaticCh <- n:
	case <-d.ctx.Done():
	}
}

func (d *dialScheduler) removeStatic(n *Node) {
	select {
	case d.remStaticCh <- n:
	case <-d.ctx.Done():
	}
}

func (d *dialScheduler) peerAdded(c *conn) {
	select {
	case d.addPeerCh <- c:
	case <-d.ctx.Done():
	}
}

func (d *dialScheduler) peerRemoved(c *conn) {
	select {
	case d.remPeerCh <- c:
	case <-d.ctx.Done():
	}
}

func (d *dialScheduler) loop() {
	defer d.wg.Done()

	for {
		slots := d.freeDialSlots()
		slots -= d.startStaticDials(slots)
		
		d.logStats()

		select {
		case task := <-d.doneCh:
			id := task.dest().ID()
			delete(d.dialing, id)
			d.updateStaticPool(id)
			d.doneSinceLastLog++

		case c := <-d.addPeerCh:
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeers++
			}
			id := c.node.ID()
			d.peers[id] = struct{}{}
			task := d.static[id]
			if task != nil && task.staticPoolIndex >= 0 {
				d.removeFromStaticPool(task.staticPoolIndex)
			}

		case c := <-d.remPeerCh:
			if c.is(dynDialedConn) || c.is(staticDialedConn) {
				d.dialPeers--
			}
			delete(d.peers, c.node.ID())
			d.updateStaticPool(c.node.ID())

		case node := <-d.addStaticCh:
			id := node.ID()
			_, exists := d.static[id]
			if exists {
				continue
			}
			task := newDialTask(node, staticDialedConn)
			d.static[id] = task
			if d.checkDial(node) == nil {
				d.addToStaticPool(task)
			}

		case node := <-d.remStaticCh:
			id := node.ID()
			task := d.static[id]
			if task != nil {
				delete(d.static, id)
				if task.staticPoolIndex >= 0 {
					d.removeFromStaticPool(task.staticPoolIndex)
				}
			}

		case <-d.historyTimer.C():
			d.expireHistory()

		case <-d.ctx.Done():
			return
		}
	}
}

func (d *dialScheduler) logStats() {
	if time.Since(d.lastStatsLog) < dialStatsLogInterval {
		return
	}
	d.doneSinceLastLog = 0
	d.lastStatsLog = time.Now()
}

func (d *dialScheduler) rearmHistoryTimer() {
	if len(d.history) == 0 {
		return
	}
	d.historyTimer.Reset(time.Until(d.history.nextExpiry()))
}

func (d *dialScheduler) expireHistory() {
	d.history.expire(time.Now(), func(hkey string) {
		var id NodeID
		copy(id[:], hkey)
		d.updateStaticPool(id)
	})
}

func (d *dialScheduler) freeDialSlots() int {
	slots := (d.maxDialPeers - d.dialPeers) * 2
	if slots > d.maxActiveDials {
		slots = d.maxActiveDials
	}
	return slots - len(d.dialing)
}

func (d *dialScheduler) checkDial(n *Node) error {
	if n.ID() == d.self {
		return errSelf
	}
	if n.TCPPort == 0 {
		return errNoPort
	}
	if _, ok := d.dialing[n.ID()]; ok {
		return errAlreadyDialing
	}
	if _, ok := d.peers[n.ID()]; ok {
		return errAlreadyConnected
	}
	if d.history.contains(string(n.ID().Bytes())) {
		return errRecentlyDialed
	}
	return nil
}

func (d *dialScheduler) startStaticDials(n int) (started int) {
	for started = 0; started < n && len(d.staticPool) > 0; started++ {
		idx := d.rand.Intn(len(d.staticPool))
		task := d.staticPool[idx]
		d.startDial(task)
		d.removeFromStaticPool(idx)
	}
	return started
}

func (d *dialScheduler) updateStaticPool(id NodeID) {
	task, ok := d.static[id]
	if ok && task.staticPoolIndex < 0 && d.checkDial(task.dest()) == nil {
		d.addToStaticPool(task)
	}
}

func (d *dialScheduler) addToStaticPool(task *dialTask) {
	if task.staticPoolIndex >= 0 {
		panic("attempt to add task to staticPool twice")
	}
	d.staticPool = append(d.staticPool, task)
	task.staticPoolIndex = len(d.staticPool) - 1
}

func (d *dialScheduler) removeFromStaticPool(idx int) {
	task := d.staticPool[idx]
	end := len(d.staticPool) - 1
	d.staticPool[idx] = d.staticPool[end]
	d.staticPool[idx].staticPoolIndex = idx
	d.staticPool[end] = nil
	d.staticPool = d.staticPool[:end]
	task.staticPoolIndex = -1
}

func (d *dialScheduler) startDial(task *dialTask) {
	node := task.dest()
	hkey := string(node.ID().Bytes())
	d.history.add(hkey, time.Now().Add(dialHistoryExpiration))
	d.dialing[node.ID()] = task
	go func() {
		task.run(d)
		d.doneCh <- task
	}()
}

type dialTask struct {
	staticPoolIndex int
	flags           connFlag
	destPtr         atomic.Pointer[Node]
	lastResolved    time.Time
	resolveDelay    time.Duration
}

func newDialTask(dest *Node, flags connFlag) *dialTask {
	t := &dialTask{flags: flags, staticPoolIndex: -1}
	t.destPtr.Store(dest)
	return t
}

type dialError struct {
	error
}

func (t *dialTask) dest() *Node {
	return t.destPtr.Load()
}

func (t *dialTask) run(d *dialScheduler) {
	if t.needResolve() && !t.resolve(d) {
		return
	}
	_ = t.dial(d, t.dest())
}

func (t *dialTask) needResolve() bool {
	return t.flags&staticDialedConn != 0 && t.dest().IP == nil
}

func (t *dialTask) resolve(d *dialScheduler) bool {
	if d.resolver == nil {
		return false
	}
	if t.resolveDelay == 0 {
		t.resolveDelay = initialResolveDelay
	}
	if !t.lastResolved.IsZero() && time.Since(t.lastResolved) < t.resolveDelay {
		return false
	}

	node := t.dest()
	resolved := d.resolver.Resolve(node)
	t.lastResolved = time.Now()
	if resolved == nil {
		t.resolveDelay *= 2
		if t.resolveDelay > maxResolveDelay {
			t.resolveDelay = maxResolveDelay
		}
		return false
	}
	t.resolveDelay = initialResolveDelay
	t.destPtr.Store(resolved)
	return true
}

func (t *dialTask) dial(d *dialScheduler, dest *Node) error {
	fd, err := d.dialer.Dial(d.ctx, dest)
	if err != nil {
		return &dialError{err}
	}
	return d.setupFunc(fd, t.flags, dest)
}

func (t *dialTask) String() string {
	node := t.dest()
	id := node.ID()
	return fmt.Sprintf("%v %x %v:%d", t.flags, id[:8], node.IP, node.TCPPort)
}

func cleanupDialErr(err error) error {
	if netErr, ok := err.(*net.OpError); ok && netErr.Op == "dial" {
		return netErr.Err
	}
	return err
}
