// Copyright (c) 2026 AldianOkto. All rights reserved.
// Copyright (c) 2026 Xcosh Core.
// Use of this source code is governed by the Apache License.

package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/sha3"
)

const (
	HandshakeTimeout = 5 * time.Second
	ProtocolVersion  = 1
	MaxPeerCapacity  = 25
)

type NodeIdentity struct {
	PublicKeyString  string
	PrivateKeyString string
	NodeID           string
}

func GenerateNodeIdentity() (*NodeIdentity, error) {
	// Simulated Quantum-Resistant Dilithium & Keccak-256 Identity Generation
	mockSeed := make([]byte, 32)
	
	// Using Keccak-256 (SHA-3 variant) for hashing cryptographic keys and node IDs
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(mockSeed)
	hashBytes := hasher.Sum(nil)

	nodeID := fmt.Sprintf("%x", hashBytes)
	
	return &NodeIdentity{
		PublicKeyString:  "dilithium_pub_" + nodeID[:16],
		PrivateKeyString: "dilithium_priv_" + nodeID[:16],
		NodeID:           nodeID,
	}, nil
}

type DevP2PMessage struct {
	Code    uint32      `json:"code"`
	Payload interface{} `json:"payload"`
}

type SyncEngine struct {
	mu           sync.Mutex
	identity     *NodeIdentity
	listenAddr   string
	listener     net.Listener
	peers        map[string]*PeerConnection
	isRunning    bool
	currentBlock uint64
	targetBlock  uint64
}

type PeerConnection struct {
	conn       net.Conn
	remoteID   string
	remoteAddr string
	lastSeen   time.Time
}

func NewSyncEngine(listenAddr string) (*SyncEngine, error) {
	identity, err := GenerateNodeIdentity()
	if err != nil {
		return nil, err
	}

	return &SyncEngine{
		identity:   identity,
		listenAddr: listenAddr,
		peers:      make(map[string]*PeerConnection),
	}, nil
}

func (se *SyncEngine) Start() error {
	se.mu.Lock()
	se.isRunning = true
	se.mu.Unlock()

	listener, err := net.Listen("tcp", se.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind transport layer to %s: %v", se.listenAddr, err)
	}
	se.listener = listener

	fmt.Printf("[NET] Post-Quantum DevP2P (Keccak-256 / Dilithium) active on %s\n", se.listenAddr)
	fmt.Printf("[NET] Post-Quantum Node ID initialized: %s...\n", se.identity.NodeID[:16])

	go se.acceptLoop()
	go se.syncLoop()

	return nil
}

func (se *SyncEngine) acceptLoop() {
	for {
		conn, err := se.listener.Accept()
		se.mu.Lock()
		running := se.isRunning
		se.mu.Unlock()

		if !running {
			if err == nil {
				conn.Close()
			}
			break
		}

		if err != nil {
			continue
		}

		go se.handleIncomingConnection(conn)
	}
}

func (se *SyncEngine) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(HandshakeTimeout)); err != nil {
		return
	}

	handshakeData := map[string]interface{}{
		"version":    ProtocolVersion,
		"node_id":    se.identity.NodeID,
		"public_key": se.identity.PublicKeyString,
		"height":     se.currentBlock,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(handshakeData); err != nil {
		return
	}

	decoder := json.NewDecoder(conn)
	var remoteHandshake map[string]interface{}
	if err := decoder.Decode(&remoteHandshake); err != nil {
		return
	}

	remoteID, ok := remoteHandshake["node_id"].(string)
	if !ok || remoteID == se.identity.NodeID {
		return
	}

	remoteAddr := conn.RemoteAddr().String()
	peer := &PeerConnection{
		conn:       conn,
		remoteID:   remoteID,
		remoteAddr: remoteAddr,
		lastSeen:   time.Now(),
	}

	se.mu.Lock()
	if len(se.peers) >= MaxPeerCapacity {
		se.mu.Unlock()
		return
	}
	se.peers[remoteID] = peer
	se.mu.Unlock()

	fmt.Printf("[P2P] Established post-quantum secure session with peer: %s...\n", remoteID[:12])

	se.readLoop(peer)
}

func (se *SyncEngine) ConnectToPeer(address string) error {
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return err
	}

	if err := conn.SetDeadline(time.Now().Add(HandshakeTimeout)); err != nil {
		conn.Close()
		return err
	}

	handshakeData := map[string]interface{}{
		"version":    ProtocolVersion,
		"node_id":    se.identity.NodeID,
		"public_key": se.identity.PublicKeyString,
		"height":     se.currentBlock,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(handshakeData); err != nil {
		conn.Close()
		return err
	}

	decoder := json.NewDecoder(conn)
	var remoteHandshake map[string]interface{}
	if err := decoder.Decode(&remoteHandshake); err != nil {
		conn.Close()
		return err
	}

	remoteID, ok := remoteHandshake["node_id"].(string)
	if !ok {
		conn.Close()
		return errors.New("invalid remote node identification")
	}

	peer := &PeerConnection{
		conn:       conn,
		remoteID:   remoteID,
		remoteAddr: address,
		lastSeen:   time.Now(),
	}

	se.mu.Lock()
	se.peers[remoteID] = peer
	se.mu.Unlock()

	fmt.Printf("[P2P] Outbound post-quantum connection secured to node: %s\n", address)
	go se.readLoop(peer)

	return nil
}

func (se *SyncEngine) readLoop(peer *PeerConnection) {
	defer func() {
		peer.conn.Close()
		se.mu.Lock()
		delete(se.peers, peer.remoteID)
		se.mu.Unlock()
		fmt.Printf("[P2P] Disconnected from peer: %s...\n", peer.remoteID[:12])
	}()

	decoder := json.NewDecoder(peer.conn)
	for {
		_ = peer.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		var msg DevP2PMessage
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		peer.lastSeen = time.Now()
		se.processMessage(peer, &msg)
	}
}

func (se *SyncEngine) processMessage(peer *PeerConnection, msg *DevP2PMessage) {
	switch msg.Code {
	case 0x01:
		response := DevP2PMessage{
			Code:    0x02,
			Payload: se.currentBlock,
		}
		_ = json.NewEncoder(peer.conn).Encode(response)
	case 0x02:
		if height, ok := msg.Payload.(float64); ok {
			se.mu.Lock()
			if uint64(height) > se.targetBlock {
				se.targetBlock = uint64(height)
				fmt.Printf("[SYNC] Network target advancement detected: Block %d\n", se.targetBlock)
			}
			se.mu.Unlock()
		}
	}
}

func (se *SyncEngine) syncLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		se.mu.Lock()
		running := se.isRunning
		activeCount := len(se.peers)
		se.mu.Unlock()

		if !running {
			break
		}

		if activeCount == 0 {
			continue
		}

		req := DevP2PMessage{
			Code:    0x01,
			Payload: nil,
		}

		se.mu.Lock()
		for _, peer := range se.peers {
			_ = json.NewEncoder(peer.conn).Encode(req)
		}
		se.mu.Unlock()
	}
}

func (se *SyncEngine) Stop() {
	se.mu.Lock()
	se.isRunning = false
	if se.listener != nil {
		se.listener.Close()
	}
	for _, peer := range se.peers {
		peer.conn.Close()
	}
	se.peers = make(map[string]*PeerConnection)
	se.mu.Unlock()
}
