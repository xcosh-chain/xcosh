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
	"fmt"
	"net"
	"time"
)

// DefaultHardcodedSeeds defines the list of official or community-backed bootstrap seed nodes for Xcosh Core.
var DefaultHardcodedSeeds = []string{
	"seed1.xcosh.org:19333",
	"seed2.xcosh.org:19333",
	"fallback.xcosh.network:19333",
	"127.0.0.1:19333",
}

// DnsSeedDomains defines the list of DNS seed domains for dynamic peer discovery (BIP 155 style).
var DnsSeedDomains = []string{
	"seed.xcosh.io",
	"dnsseed.xcosh.org",
}

// AutoDiscoverAndConnect performs automatic discovery and connection attempts to hardcoded seeds and DNS seeds.
func (s *Server) AutoDiscoverAndConnect(customConnect string) {
	// If the user manually specifies a --connect argument, give it priority.
	if customConnect != "" {
		fmt.Printf("[P2P-SEED] Connecting to manual override peer: %s\n", customConnect)
		go func() {
			if err := s.ConnectToPeer(customConnect); err != nil {
				fmt.Printf("[P2P-SEED] Failed to connect to manual peer %s: %v\n", customConnect, err)
			}
		}()
		return
	}

	fmt.Println("[P2P-SEED] No manual peer specified. Initiating automated seed bootstrap...")

	// Connect to Hardcoded Seeds asynchronously
	for _, seed := range DefaultHardcodedSeeds {
		go func(addr string) {
			fmt.Printf("[P2P-SEED] Attempting bootstrap connection to hardcoded seed: %s\n", addr)
			time.Sleep(1 * time.Second)
			if err := s.ConnectToPeer(addr); err != nil {
				fmt.Printf("[P2P-SEED] Seed %s is currently offline/unreachable.\n", addr)
			} else {
				fmt.Printf("[P2P-SEED] Successfully connected to seed node: %s\n", addr)
			}
		}(seed)
	}

	// Perform DNS Seed Resolution (BIP 155 Dynamic Lookup)
	go func() {
		for _, domain := range DnsSeedDomains {
			fmt.Printf("[P2P-SEED] Resolving DNS seed domain: %s\n", domain)
			ips, err := net.LookupIP(domain)
			if err != nil {
				fmt.Printf("[P2P-SEED] DNS seed resolution failed for %s: %v\n", domain, err)
				continue
			}

			for _, ip := range ips {
				peerAddr := fmt.Sprintf("%s:19333", ip.String())
				fmt.Printf("[P2P-SEED] Discovered peer via DNS lookup: %s\n", peerAddr)
				go func(target string) {
					_ = s.ConnectToPeer(target)
				}(peerAddr)
			}
		}
	}()
}
