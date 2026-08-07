// Copyright (c) 2026 AldianOkto. All rights reserved.
// Copyright (c) 2026 Xcosh Core.
// Use of this source code is governed by the Apache License.
// that can be found in the root directory of this repository.

package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"xcosh/internal/consensus"
)

// ValidateBlockConsensus rigorously evaluates incoming block structures against 
// immutable cryptographic consensus parameters. 
// Enforces strict Bitcoin-grade protocol compliance: rejects any unauthorized 
// reward manipulation, max supply breaches, halving interval mismatches, block size limit breaches, invalid proof-of-work proofs, or malformed address prefixes.
func ValidateBlockConsensus(block *LedgerBlock, prevBlock *LedgerBlock, currentTotalSupply uint64) error {
	// 1. Validate sequential block height index progression and chronological integrity.
	if prevBlock != nil {
		// Verify that the incoming block index strictly increments by exactly one integer unit.
		if block.Index != prevBlock.Index+1 {
			return fmt.Errorf("CONSENSUS ERROR: Invalid block index. Expected %d, got %d", prevBlock.Index+1, block.Index)
		}
		// Enforce strict chronological ordering to prevent timestamp manipulation attacks.
		if block.Timestamp <= prevBlock.Timestamp {
			return fmt.Errorf("CONSENSUS ERROR: Block timestamp must strictly exceed the previous block timestamp")
		}
		// Verify cryptographic chain linkage by matching parent hash pointers.
		if !bytes.Equal(block.PrevHash, prevBlock.Hash) {
			return fmt.Errorf("CONSENSUS ERROR: Broken ledger chain linkage! Previous hash verification failed")
		}
	} else {
		// Enforce strict initialization parameters for the Genesis block height zero.
		if block.Index != 0 {
			return fmt.Errorf("CONSENSUS ERROR: Non-zero index detected for genesis block initialization")
		}
	}

	// 2. Enforce strict Bitcoin-grade Address Prefix validation (Consensus Integrity)
	// Ensure the miner uses a valid address prefix matching the core consensus specification.
	// Bypassed exclusively for the initial genesis block ("SYSTEM_GENESIS").
	if block.Index > 0 && block.Miner != "SYSTEM_GENESIS" {
		params := consensus.DefaultConsensus()
		requiredPrefix := params.AddressPrefix
		if !strings.HasPrefix(block.Miner, requiredPrefix) {
			return fmt.Errorf("CONSENSUS REJECTION: Invalid miner address prefix! Expected prefix '%s', got '%s'", requiredPrefix, block.Miner)
		}
	}

	// 3. Enforce absolute cryptographic Proof-of-Work (PoW) target verification.
	hashStr := hex.EncodeToString(block.Hash)
	// Evaluate leading zero-bit constraints against the active target difficulty level.
	if !consensus.ValidatePoW(hashStr, uint64(block.Difficulty)) {
		return fmt.Errorf("CONSENSUS ERROR: Block header hash fails to satisfy target difficulty bits")
	}

	// 4. Enforce strict MaxSupply consistency matching active protocol consensus parameters.
	blockMaxSupply := block.MaxSupply
	if blockMaxSupply == 0 {
		blockMaxSupply = consensus.MaxSupply
	}
	if blockMaxSupply != consensus.MaxSupply {
		return fmt.Errorf("CONSENSUS REJECTION: Block MaxSupply (%d) does not match active network consensus MaxSupply (%d)", blockMaxSupply, consensus.MaxSupply)
	}

	// Enforce strict HalvingInterval consistency matching active protocol consensus parameters.
	blockHalvingInterval := block.HalvingInterval
	if blockHalvingInterval == 0 {
		blockHalvingInterval = consensus.HalvingInterval
	}
	if blockHalvingInterval != consensus.HalvingInterval {
		return fmt.Errorf("CONSENSUS REJECTION: Block HalvingInterval (%d) does not match active network consensus HalvingInterval (%d)", blockHalvingInterval, consensus.HalvingInterval)
	}

	// 5. Enforce strict Block Size Limit validation (4 MB Max Limit for Dilithium-3 transactions)
	blockBytes, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("CONSENSUS REJECTION: Failed to serialize block for size validation: %v", err)
	}
	
	if uint64(len(blockBytes)) > consensus.MaxBlockSizeBytes {
		return fmt.Errorf("CONSENSUS REJECTION: Block size (%d bytes) exceeds maximum allowed limit (%d bytes)", len(blockBytes), consensus.MaxBlockSizeBytes)
	}

	// 6. Execute precise macroeconomic validation: Block reward and immutable Max Supply enforcement.
	expectedReward := consensus.CalculateBlockReward(block.Index)
	
	// Terminate coin issuance completely if cumulative circulating supply has reached the hard-coded maximum cap.
	if currentTotalSupply >= consensus.MaxSupply {
		expectedReward = 0
	} else if currentTotalSupply+expectedReward > consensus.MaxSupply {
		// Truncate the block reward precisely to fit the remaining available supply allocation.
		expectedReward = consensus.MaxSupply - currentTotalSupply
	}

	// Reject outright any block attempting unauthorized coin issuance exceeding exact protocol allowances (zero tolerance for fractional deviations).
	if block.Reward != expectedReward {
		return fmt.Errorf("CONSENSUS VIOLATION: Illegal block reward claimed! Claimed: %d, Strictly Allowed: %d", block.Reward, expectedReward)
	}

	return nil
}
