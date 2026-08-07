// Copyright (c) 2026 AldianOkto. All rights reserved.
// Copyright (c) 2026 Xcosh Core.
// Use of this source code is governed by the Apache License.
// that can be found in the root directory of this repository.
// Project: Xcosh / Blockchain Core
//
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at. <http://www.apache.org/licenses/LICENSE-2.0>
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package p2p

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"xcosh/core"
)

// MessageType defines the classification of network messages transmitted across nodes.
type MessageType string

const (
	MsgTx    MessageType = "TX"
	MsgBlock MessageType = "BLOCK"
)

// Envelope wraps network payloads with a specific type header for transmission parsing.
type Envelope struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Server represents the P2P networking node manager.
type Server struct {
	Address     string
	Peers       map[string]net.Conn
	Mu          sync.Mutex
	mux         *http.ServeMux
	AddrManager *AddrManager
}

// getDataDirInternal resolves the local node configuration path for persistent storage.
func getDataDirInternal() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		dir := filepath.Join(".", ".xcosh")
		os.MkdirAll(dir, 0755)
		return dir
	}
	dir := filepath.Join(homeDir, ".xcosh")
	os.MkdirAll(dir, 0755)
	return dir
}

// NewServer initializes a new P2P network server instance along with the address manager.
func NewServer(address string) *Server {
	mux := http.NewServeMux()
	dataDir := getDataDirInternal()
	return &Server{
		Address:     address,
		Peers:       make(map[string]net.Conn),
		mux:         mux,
		AddrManager: NewAddrManager(dataDir),
	}
}

// Mux returns the internal HTTP ServeMux for registering RPC/API endpoints.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// StartListening binds to the specified TCP address and listens for incoming node connections.
func (s *Server) StartListening(onBlockReceived func(*core.LedgerBlock), onTxReceived func(*core.Transfer)) error {
	// Bind a TCP network listener to the configured server address endpoint.
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("[P2P] Node networking server listening on %s...\n", s.Address)

	// Continuously accept incoming TCP connection requests from remote peer nodes.
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		
		// Automatically record incoming peer remote address into AddrManager if possible.
		if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
			if tcpAddr, ok := remoteAddr.(*net.TCPAddr); ok {
				s.AddrManager.AddAddress(tcpAddr.IP.String(), tcpAddr.Port)
			}
		}

		// Spawn a dedicated background goroutine to handle communication for each accepted connection.
		go s.handleConnection(conn, onBlockReceived, onTxReceived)
	}
}

// handleConnection processes incoming message streams from established peer connections.
func (s *Server) handleConnection(conn net.Conn, onBlock func(*core.LedgerBlock), onTx func(*core.Transfer)) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	// Continuously decode incoming message envelopes from the active network stream.
	for {
		var env Envelope
		if err := decoder.Decode(&env); err != nil {
			break
		}

		// Route incoming messages based on their designated envelope type classification.
		switch env.Type {
		case MsgTx:
			var tx core.Transfer
			if json.Unmarshal(env.Payload, &tx) == nil && onTx != nil {
				onTx(&tx)
			}
		case MsgBlock:
			var block core.LedgerBlock
			if json.Unmarshal(env.Payload, &block) == nil && onBlock != nil {
				onBlock(&block)
			}
		}
	}
}

// Broadcast distributes transaction or block payloads to all connected network peers.
func (s *Server) Broadcast(msgType MessageType, data interface{}) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// Serialize the target data payload into a raw JSON byte slice.
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}

	// Wrap the payload into a network message envelope container structure.
	envBytes, err := json.Marshal(Envelope{
		Type:    msgType,
		Payload: payload,
	})
	if err != nil {
		return
	}

	// Iterate through all active peer connections and transmit the serialized envelope data stream.
	for addr, conn := range s.Peers {
		_, err := conn.Write(append(envBytes, '\n'))
		if err != nil {
			conn.Close()
			delete(s.Peers, addr)
		}
	}
}

// ConnectToPeer establishes an outbound TCP connection to another active network peer.
func (s *Server) ConnectToPeer(peerAddr string) error {
	// Dial an outbound TCP network connection to the specified remote peer address.
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return err
	}

	s.Mu.Lock()
	// Store the established connection inside the active peer tracking map.
	s.Peers[peerAddr] = conn
	s.Mu.Unlock()

	// Parse host and port to register into AddrManager database
	if host, portStr, err := net.SplitHostPort(peerAddr); err == nil {
		var port int
		fmt.Sscanf(portStr, "%d", &port)
		if s.AddrManager != nil {
			s.AddrManager.AddAddress(host, port)
		}
	}

	fmt.Printf("[P2P] Successfully connected to remote peer: %s\n", peerAddr)
	return nil
}

// GetPeerCount returns the total count of currently connected peer nodes.
func (s *Server) GetPeerCount() int {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	// Return the total number of items present in the active peer connections map.
	return len(s.Peers)
}

// GetPeerList returns a list of active peer IP/port addresses.
func (s *Server) GetPeerList() []string {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	
	// Collect and return all active peer address keys from the connection tracking map.
	peers := make([]string, 0, len(s.Peers))
	for addr := range s.Peers {
		peers = append(peers, addr)
	}
	return peers
}
