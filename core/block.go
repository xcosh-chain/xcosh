// Copyright (c) 2026 AldianOkto. All rights reserved.
// Copyright (c) 2026 Xcosh Core.
// Use of this source code is governed by the Apache License.
// that can be found in the root directory of this repository.
// Project: Xcosh / Blockchain Core
//
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"encoding/hex"
	"runtime"
	"strconv"
	"sync/atomic"

	"xcosh/internal/consensus"
	"golang.org/x/crypto/sha3"
)

// CoinUnit defines an 8-decimal scaling factor retrieved directly from consensus parameters.
const CoinUnit = consensus.CoinUnit

const MaxXcoshSupply uint64 = consensus.MaxSupply / consensus.CoinUnit // Aligns with MaxSupply specified in internal/consensus

// LedgerBlock represents the core structural block entity containing transactional ledger data, cryptographic hashes, and consensus metadata.
type LedgerBlock struct {
	Index           uint64      `json:"index"`
	Timestamp       int64       `json:"timestamp"`
	PrevHash        []byte      `json:"prev_hash"`
	Hash            []byte      `json:"hash"`
	Transfers       []*Transfer `json:"transfers"`
	Miner           string      `json:"miner"`
	Nonce           uint64      `json:"nonce"`
	Difficulty      uint32      `json:"difficulty"`
	Bits            uint32      `json:"bits"`             // Compact target difficulty bits representation (nBits)
	Reward          uint64      `json:"reward"`
	MaxSupply       uint64      `json:"max_supply"`       // Bound directly to enable true cryptographic sovereign hard fork sensitivity
	HalvingInterval uint64      `json:"halving_interval"` // Bound directly to alter PoW fingerprint on halving policy change
	Message         string      `json:"message,omitempty"` // Added pszTimestamp equivalent field
}

// GetBlockReward dynamically computes the block reward using the centralized consensus package.
func GetBlockReward(blockHeight uint64) uint64 {
	return consensus.CalculateBlockReward(blockHeight)
}

// ConsensusEngine coordinates the proof-of-work mining process, hash target difficulty evaluation, and block validation parameters.
type ConsensusEngine struct {
	TargetDifficulty uint32
	Bits             uint32
}

// NewConsensusEngine initializes and returns a new ConsensusEngine instance configured with the specified difficulty target and bits.
func NewConsensusEngine(difficulty uint32) *ConsensusEngine {
	// Instantiate and return a ConsensusEngine pointer with the targeted difficulty level and default genesis bits.
	return &ConsensusEngine{
		TargetDifficulty: difficulty,
		Bits:             consensus.GenesisBits,
	}
}

// AssembleBlockData serializes and concatenates block headers, transactional payloads, reward, max supply, halving interval, candidate nonce, and genesis message into a unified byte array for hashing.
func (ce *ConsensusEngine) AssembleBlockData(b *LedgerBlock, nonce uint64) []byte {
	var rawTxData []byte
	// Concatenate all transfer signatures included in the block payload.
	for _, tx := range b.Transfers {
		rawTxData = append(rawTxData, tx.Signature...)
	}

	// Fallback to active consensus MaxSupply if block struct field is uninitialized
	blockMaxSupply := b.MaxSupply
	if blockMaxSupply == 0 {
		blockMaxSupply = consensus.MaxSupply
	}

	// Fallback to active consensus HalvingInterval if block struct field is uninitialized
	blockHalvingInterval := b.HalvingInterval
	if blockHalvingInterval == 0 {
		blockHalvingInterval = consensus.HalvingInterval
	}

	// Join all block components including Reward, MaxSupply, HalvingInterval, and Message into a single canonical byte array representation.
	return bytes.Join([][]byte{
		b.PrevHash,
		rawTxData,
		[]byte(strconv.FormatUint(b.Index, 16)),
		[]byte(strconv.FormatInt(b.Timestamp, 16)),
		[]byte(strconv.FormatUint(b.Reward, 16)),         // Include reward for macro-economic hard fork sensitivity
		[]byte(strconv.FormatUint(blockMaxSupply, 16)),        // Include block-bound max supply for true sovereign hard fork sensitivity
		[]byte(strconv.FormatUint(blockHalvingInterval, 16)),   // Include halving interval to trigger hash fingerprint changes on modification
		[]byte(strconv.FormatUint(uint64(b.Difficulty), 16)),
		[]byte(strconv.FormatUint(nonce, 16)),
		[]byte(b.Message), // Include message in PoW hashing calculation
	}, []byte{})
}

// Mine executes a multi-threaded parallel Proof-of-Work search loop utilizing all available CPU cores.
func (ce *ConsensusEngine) Mine(b *LedgerBlock) (uint64, []byte) {
	// Preserve the immutable block reward configured and validated by LedgerCore to enforce hard MaxSupply caps.
	// Fall back to standard block reward calculation exclusively if uninitialized outside genesis bounds.
	if b.Reward == 0 && b.Index > 0 {
		b.Reward = GetBlockReward(b.Index)
	}

	// Ensure block MaxSupply reflects active consensus rule if uninitialized
	if b.MaxSupply == 0 {
		b.MaxSupply = consensus.MaxSupply
	}

	// Ensure block HalvingInterval reflects active consensus rule if uninitialized
	if b.HalvingInterval == 0 {
		b.HalvingInterval = consensus.HalvingInterval
	}

	// Ensure the block carries the proper bits configuration
	if b.Bits == 0 {
		b.Bits = ce.Bits
	}

	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 4
	}

	// Channel to capture the successful mining result (nonce and hash)
	type result struct {
		nonce uint64
		hash  []byte
	}
	resultChan := make(chan result, 1)
	stopChan := make(chan struct{})

	// Atomic counter to distribute nonce ranges cleanly across worker goroutines
	var baseNonce uint64 = 0

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			// Each worker gets its own independent Keccak-256 hasher instance to avoid data races
			hasher := sha3.NewLegacyKeccak256()
			
			// Local chunk offset per worker
			var localNonce uint64 = uint64(workerID) * 1000000000

			for {
				select {
				case <-stopChan:
					return
				default:
					// Periodically grab a block of nonces atomically
					if localNonce%50000 == 0 {
						localNonce = atomic.AddUint64(&baseNonce, 50000)
					}

					data := ce.AssembleBlockData(b, localNonce)
					hasher.Reset()
					hasher.Write(data)
					hash := hasher.Sum(nil)

					// Evaluate generated hash against active consensus difficulty target constraints.
					if ce.validateHash(hash) {
						select {
						case resultChan <- result{nonce: localNonce, hash: hash}:
						default:
						}
						return
					}
					localNonce++
				}
			}
		}(i)
	}

	// Wait until a worker finds the valid hash
	res := <-resultChan
	close(stopChan) // Signal all other worker goroutines to stop

	return res.nonce, res.hash
}

// validateHash validates the generated hash using validation rules defined within internal/consensus.
func (ce *ConsensusEngine) validateHash(hash []byte) bool {
	hashStr := hex.EncodeToString(hash)
	// Directly invoke the ValidatePoW function provided by the internal/consensus package.
	return consensus.ValidatePoW(hashStr, uint64(ce.TargetDifficulty))
}
