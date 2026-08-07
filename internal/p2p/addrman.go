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
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PeerAddress represents the network connection metadata of a remote peer node.
type PeerAddress struct {
	IP        string    `json:"ip"`
	Port      int       `json:"port"`
	Services  uint64    `json:"services"`
	LastSeen  time.Time `json:"last_seen"`
	Attempts  int       `json:"attempts"`
}

// AddrManager manages the database of discovered peer addresses, supporting decentralized peer discovery and persistence.
type AddrManager struct {
	mu        sync.Mutex
	Addresses map[string]*PeerAddress // Key format: "ip:port"
	FilePath  string
}

// NewAddrManager initializes the address manager instance and loads historical peer records from disk if available.
func NewAddrManager(dataDir string) *AddrManager {
	am := &AddrManager{
		Addresses: make(map[string]*PeerAddress),
		FilePath:  filepath.Join(dataDir, "peers_addrman.json"),
	}
	am.LoadFromDisk()
	return am
}

// AddAddress registers a newly discovered peer address or updates its last seen timestamp if it already exists.
func (am *AddrManager) AddAddress(ip string, port int) {
	am.mu.Lock()
	defer am.mu.Unlock()

	key := fmt.Sprintf("%s:%d", ip, port)
	if existing, exists := am.Addresses[key]; exists {
		existing.LastSeen = time.Now()
	} else {
		am.Addresses[key] = &PeerAddress{
			IP:       ip,
			Port:     port,
			LastSeen: time.Now(),
		}
	}
	// Persist the updated address collection to disk.
	am.SaveToDisk()
}

// GetKnownAddresses returns a slice of all currently tracked peer address strings.
func (am *AddrManager) GetKnownAddresses() []string {
	am.mu.Lock()
	defer am.mu.Unlock()

	var list []string
	for key := range am.Addresses {
		list = append(list, key)
	}
	return list
}

// SaveToDisk serializes the active address database and writes it safely to local persistent storage.
func (am *AddrManager) SaveToDisk() {
	data, err := json.MarshalIndent(am.Addresses, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(am.FilePath, data, 0644)
}

// LoadFromDisk reads and unmarshals the persisted peer address dataset from local storage into memory.
func (am *AddrManager) LoadFromDisk() {
	data, err := os.ReadFile(am.FilePath)
	if err != nil {
		return
	}
	_ = json.Unmarshal(data, &am.Addresses)
}
