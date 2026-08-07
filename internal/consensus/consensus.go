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

package consensus

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"

	"golang.org/x/crypto/sha3"
)

// PoWLimit defines the maximum target difficulty limit in Big Integer 256-bit representation.
var PoWLimit, _ = new(big.Int).SetString("00000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)

// Immutable Network & Macroeconomic Constants (Hardcoded Rules)
const (
	CoinUnit          uint64 = 100000000             // 8 Decimals precision factor
	MaxSupply         uint64 = 785000000 * CoinUnit // Fixed Maximum Cap: 785 Million Units
	BlockReward       uint64 = 50 * CoinUnit         // Initial Minting Reward: 50 Units per Block
	HalvingInterval   uint64 = 7850000               // Strict Halving Block Interval
	DefaultPort       int    = 19333                 // Default P2P Network Port
	AddressPrefix     string = "xcosh"                // Immutable Wallet Address Prefix
	GenesisBits       uint32 = 0x1e0ffff0            // Compact difficulty bits representation
	MaxBlockSizeBytes uint64 = 4 * 1024 * 1024       // Maximum Block Size Limit (4 MB for Dilithium-3 transactions)

	// Proof-of-Work Target Parameters
	PowTargetTimespan   int64 = 2 * 24 * 60 * 60  
	PowTargetSpacing    int64 = 35                     
	TargetBlockTimeSec  int64 = PowTargetSpacing   

	// ExpectedGenesisHash stores the immutable cryptographic hash checkpoint.
	// Jika MaxSupply atau BlockReward diubah, hash ini wajib disesuaikan atau node akan menolak rantai.
	ExpectedGenesisHash string = "00000cac47528e27627a256f5ba877e4768d22417fd873532ffdea8d0c7be77a"
)

// HardcodedCheckpoints stores trusted historical checkpoints.
var HardcodedCheckpoints = map[uint64]string{
	0: ExpectedGenesisHash,
}

// ConsensusParameters defines fixed macroeconomic rules.
type ConsensusParameters struct {
	DifficultyBits    uint64 
	GenesisBits       uint32 
	BlockReward       uint64 
	MaxSupply         uint64 
	HalvingInterval   uint64 
	DefaultPort       int    
	AddressPrefix     string 
	MaxBlockSizeBytes uint64 
	PowTargetTimespan int64  
	PowTargetSpacing  int64  
}

// DefaultConsensus returns standard operational consensus rules.
func DefaultConsensus() *ConsensusParameters {
	return &ConsensusParameters{
		DifficultyBits:    1,
		GenesisBits:       GenesisBits,
		BlockReward:       BlockReward,
		MaxSupply:         MaxSupply,
		HalvingInterval:   HalvingInterval,
		DefaultPort:       DefaultPort,
		AddressPrefix:     AddressPrefix,
		MaxBlockSizeBytes: MaxBlockSizeBytes,
		PowTargetTimespan: PowTargetTimespan,
		PowTargetSpacing:  PowTargetSpacing,
	}
}

// CalculateBlockReward dynamically computes the block reward based on the hardcoded halving interval.
func CalculateBlockReward(blockIndex uint64) uint64 {
	halvings := blockIndex / HalvingInterval
	if halvings >= 64 {
		return 0
	}
	return BlockReward >> halvings
}

// CalculateNextDifficulty implements a dynamic difficulty adjustment.
func CalculateNextDifficulty(prevBlockTimestamp int64, currentBlockTimestamp int64, prevDifficulty uint64) uint64 {
	if prevBlockTimestamp == 0 || currentBlockTimestamp <= prevBlockTimestamp {
		if prevDifficulty < 1 {
			return 1
		}
		return prevDifficulty
	}

	timeElapsed := currentBlockTimestamp - prevBlockTimestamp
	var newDifficulty uint64 = prevDifficulty

	if timeElapsed < TargetBlockTimeSec {
		newDifficulty = prevDifficulty + 1
	} else if timeElapsed > TargetBlockTimeSec*2 {
		if prevDifficulty > 1 {
			newDifficulty = prevDifficulty - 1
		}
	}

	if newDifficulty < 1 {
		return 1
	}

	return newDifficulty
}

// ValidatePoW verifies whether a given block header hash satisfies the target difficulty limit.
func ValidatePoW(blockHashHex string, difficultyBits uint64) bool {
	hashInt := new(big.Int)
	hashBytes, err := hex.DecodeString(blockHashHex)
	if err != nil {
		return false
	}
	hashInt.SetBytes(hashBytes)

	if hashInt.Cmp(PoWLimit) > 0 {
		return false
	}

	target := new(big.Int).Set(PoWLimit)
	if difficultyBits > 1 {
		target.Rsh(target, uint(difficultyBits))
	}

	return hashInt.Cmp(target) <= 0
}

// ComputeHeaderHash calculates the cryptographic Keccak-256 hash representation, now bound tightly with macroeconomic parameters.
func ComputeHeaderHash(prevHash string, merkleRoot string, timestamp int64, nonce uint64, message string) string {
	// Mengikat MaxSupply dan BlockReward langsung ke dalam payload hash header agar setiap perubahan ekonomi memicu hard fork mutlak ala Bitcoin!
	record := bytes.Join([][]byte{
		[]byte(prevHash),
		[]byte(merkleRoot),
		big.NewInt(timestamp).Bytes(),
		big.NewInt(int64(nonce)).Bytes(),
		[]byte(message),
		big.NewInt(int64(MaxSupply)).Bytes(),
		big.NewInt(int64(BlockReward)).Bytes(),
	}, []byte{})

	d := sha3.NewLegacyKeccak256()
	d.Write(record)
	hash := d.Sum(nil)
	
	return hex.EncodeToString(hash)
}

// VerifyGenesisCheckpoint rigorously evaluates whether a given block hash matches the immutable protocol genesis checkpoint.
func VerifyGenesisCheckpoint(blockHash []byte) error {
	actualHashHex := hex.EncodeToString(blockHash)
	if actualHashHex != ExpectedGenesisHash {
		return fmt.Errorf("CONSENSUS REJECTION: Invalid genesis block hash! Expected checkpoint '%s', got '%s'. Chain rejected due to hardcoded parameter violation.", ExpectedGenesisHash, actualHashHex)
	}
	return nil
}

// VerifyCheckpoint validates any given block height and its hash against the hardcoded historical checkpoints map.
func VerifyCheckpoint(height uint64, blockHash []byte) error {
	expectedHash, exists := HardcodedCheckpoints[height]
	if !exists {
		return nil 
	}

	actualHashHex := hex.EncodeToString(blockHash)
	if actualHashHex != expectedHash {
		return fmt.Errorf("CONSENSUS REJECTION: Checkpoint mismatch at block height %d! Expected '%s', got '%s'. Chain rejected.", height, expectedHash, actualHashHex)
	}
	return nil
}

// VerifyBlockReward strictly checks if the distributed block reward adheres to protocol limits.
func VerifyBlockReward(rewardClaimed uint64, feesCollected uint64, blockIndex uint64, currentSupply uint64) error {
	standardReward := CalculateBlockReward(blockIndex)
	
	if currentSupply >= MaxSupply {
		standardReward = 0
	} else if currentSupply+standardReward > MaxSupply {
		standardReward = MaxSupply - currentSupply
	}

	if rewardClaimed > (standardReward + feesCollected) {
		return fmt.Errorf("CONSENSUS REJECTION: Reward claimed (%d) exceeds allowed protocol limit (%d)", rewardClaimed, standardReward+feesCollected)
	}
	return nil
}

// VerifyBlockReorgTransition validates whether an incoming block for a fork/reorg complies with consensus rules.
func VerifyBlockReorgTransition(blockHeight uint64, blockHash []byte, prevHash string, currentTipHeight uint64) error {
	// 1. Verify against hardcoded historical checkpoints if any exist at this height
	if err := VerifyCheckpoint(blockHeight, blockHash); err != nil {
		return err
	}

	// 2. Prevent reorg attempts that try to rewrite history past a hard checkpoint
	for cpHeight := range HardcodedCheckpoints {
		if blockHeight <= cpHeight && blockHeight != cpHeight {
			return fmt.Errorf("CONSENSUS REJECTION: Reorg attempt violates historical checkpoint boundary at height %d", cpHeight)
		}
	}

	return nil
}
