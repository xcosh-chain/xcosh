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

package node

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"xcosh/core"
	"xcosh/crypto"
	"xcosh/internal/consensus"
	"xcosh/storage"
)

const (
	genesisTimestamp    int64  = 1770249600
	genesisBits         uint32 = 504365040
	pszTimestamp               = "anjayyy"
)

// AccountState represents the account balance and transaction sequence nonce.
type AccountState struct {
	Balance uint64 `json:"balance"`
	Nonce   uint64 `json:"nonce"`
}

// LedgerCore manages the blockchain chain state, mempool transaction queue, and block validation engine.
type LedgerCore struct {
	Mu           sync.RWMutex
	Chain        []*core.LedgerBlock
	State        map[string]*AccountState
	Mempool      []*core.Transfer
	Engine       *core.ConsensusEngine
	MinerAddress string
	Storage      *storage.Database
	StopSignal   chan bool
}

// formatCoin converts raw integer units to a floating-point representation for display.
func formatCoin(amount uint64) float64 {
	return float64(amount) / float64(consensus.CoinUnit)
}

// CalculateBlockReward delegates to the centralized consensus calculation engine.
func CalculateBlockReward(blockIndex uint64) uint64 {
	return consensus.CalculateBlockReward(blockIndex)
}

// GetTotalCirculatingSupply calculates the total cumulative coins circulating across all existing blocks.
func (lc *LedgerCore) GetTotalCirculatingSupply() uint64 {
	var totalSupply uint64 = 0
	for _, block := range lc.Chain {
		totalSupply += block.Reward
	}
	return totalSupply
}

// VerifyConsensusIntegrity verifies checkpoint rules across active blocks.
func (lc *LedgerCore) VerifyConsensusIntegrity() {
	if len(lc.Chain) == 0 {
		return
	}

	for i, block := range lc.Chain {
		if err := consensus.VerifyCheckpoint(uint64(i), block.Hash); err != nil {
			panic(fmt.Sprintf("\n[FATAL CONSENSUS PANIC] %v", err))
		}
	}

	totalStoredSupply := lc.GetTotalCirculatingSupply()
	if totalStoredSupply > consensus.MaxSupply {
		panic(fmt.Sprintf("\n\n[FATAL CONSENSUS PANIC] MACROECONOMIC RULE VIOLATION!\n"+
			"Total stored circulating supply (%.8f) exceeds current code MaxSupply limit (%.8f)!\n"+
			"Node execution halted immediately to prevent structural corruption.",
			formatCoin(totalStoredSupply), formatCoin(consensus.MaxSupply)))
	}
}

// InitializeLedger initializes or loads the local ledger database state from the specified storage path.
func InitializeLedger(dbPath string, initialDifficulty uint32, minerAddr string) *LedgerCore {
	db, err := storage.NewDatabase(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open database: %v", err))
	}

	params := consensus.DefaultConsensus()
	if initialDifficulty == 0 {
		initialDifficulty = uint32(params.DifficultyBits)
	}

	coreLedger := &LedgerCore{
		Chain:        make([]*core.LedgerBlock, 0),
		State:        make(map[string]*AccountState),
		Mempool:      make([]*core.Transfer, 0),
		Engine:       core.NewConsensusEngine(initialDifficulty),
		MinerAddress: minerAddr,
		Storage:      db,
		StopSignal:   make(chan bool),
	}

	if !coreLedger.LoadFromDisk() {
		fmt.Println("[DB] Database is empty. Spawning Genesis Block...")
		coreLedger.SpawnGenesis()
	} else {
		fmt.Println("[DB] Blockchain successfully loaded from LevelDB storage!")
	}

	return coreLedger
}

// LoadFromDisk loads existing blockchain blocks from disk storage, validates consensus parameters dynamically, and rebuilds state.
func (lc *LedgerCore) LoadFromDisk() bool {
	lastIdx, exists := lc.Storage.GetLastIndex()
	if !exists {
		return false
	}

	for i := uint64(0); i <= lastIdx; i++ {
		data, err := lc.Storage.GetBlock(i)
		if err != nil {
			break
		}
		var block core.LedgerBlock
		if err := json.Unmarshal(data, &block); err == nil {
			lc.Chain = append(lc.Chain, &block)
			lc.RebuildState(&block)
		}
	}

	if len(lc.Chain) > 0 {
		storedGenesis := lc.Chain[0]
		currentConsensusReward := consensus.BlockReward
		currentMaxSupply := consensus.MaxSupply
		currentHalvingInterval := consensus.HalvingInterval

		if storedGenesis.Message != pszTimestamp || storedGenesis.Timestamp != genesisTimestamp || storedGenesis.Bits != genesisBits || storedGenesis.Reward != currentConsensusReward || storedGenesis.MaxSupply != currentMaxSupply || storedGenesis.HalvingInterval != currentHalvingInterval {
			fmt.Println("\n================================================================================")
			fmt.Println("[CONSENSUS EVENT] Genesis parameters or macroeconomic policies modified in code! Triggering Sovereign Hard Fork...")
			fmt.Printf("OLD GENESIS -> Message: '%s' | Reward: %.8f | MaxSupply: %.8f | HalvingInterval: %d | Timestamp: %d | Bits: %d\n", storedGenesis.Message, formatCoin(storedGenesis.Reward), formatCoin(storedGenesis.MaxSupply), storedGenesis.HalvingInterval, storedGenesis.Timestamp, storedGenesis.Bits)
			fmt.Printf("NEW GENESIS -> Message: '%s' | Reward: %.8f | MaxSupply: %.8f | HalvingInterval: %d | Timestamp: %d | Bits: %d\n", pszTimestamp, formatCoin(currentConsensusReward), formatCoin(currentMaxSupply), currentHalvingInterval, genesisTimestamp, genesisBits)
			fmt.Println("[CONSENSUS EVENT] Automatically wiping storage and re-mining genesis block...")
			fmt.Println("================================================================================")

			lc.Chain = make([]*core.LedgerBlock, 0)
			lc.State = make(map[string]*AccountState)
			lc.Storage.ClearAll()
			
			lc.SpawnGenesis()
			return true
		}
	}
	
	lc.VerifyConsensusIntegrity()
	return len(lc.Chain) > 0
}

// RebuildState updates the account balances and nonces based on the transactions within a block.
func (lc *LedgerCore) RebuildState(block *core.LedgerBlock) {
	for _, tx := range block.Transfers {
		sender := crypto.PubkeyToAddress(tx.SenderPubKey)
		if _, ok := lc.State[sender]; !ok {
			lc.State[sender] = &AccountState{Balance: 0, Nonce: 0}
		}
		
		if lc.State[sender].Balance >= (tx.Value + tx.Fee) {
			lc.State[sender].Balance -= (tx.Value + tx.Fee)
		} else {
			lc.State[sender].Balance = 0
		}
		lc.State[sender].Nonce++

		if _, ok := lc.State[tx.Recipient]; !ok {
			lc.State[tx.Recipient] = &AccountState{Balance: 0, Nonce: 0}
		}
		lc.State[tx.Recipient].Balance += tx.Value
	}

	if block.Miner != "SYSTEM_GENESIS" && block.Miner != "" {
		var feeTotal uint64 = 0
		for _, tx := range block.Transfers {
			feeTotal += tx.Fee
		}
		
		totalRewardAdded := block.Reward + feeTotal
		if totalRewardAdded > 0 {
			if _, ok := lc.State[block.Miner]; !ok {
				lc.State[block.Miner] = &AccountState{Balance: 0, Nonce: 0}
			}
			lc.State[block.Miner].Balance += totalRewardAdded
		}
	}
}

// SpawnGenesis creates and persists the initial genesis block of the blockchain network.
func (lc *LedgerCore) SpawnGenesis() {
	exactReward := CalculateBlockReward(0)

	genesis := &core.LedgerBlock{
		Index:           0,
		Timestamp:       genesisTimestamp,
		PrevHash:        make([]byte, 64),
		Transfers:       []*core.Transfer{},
		Miner:           "SYSTEM_GENESIS",
		Nonce:           0,
		Difficulty:      lc.Engine.TargetDifficulty,
		Bits:            genesisBits,
		Reward:          exactReward,
		MaxSupply:       consensus.MaxSupply,
		HalvingInterval: consensus.HalvingInterval,
		Message:         pszTimestamp,
	}

	foundNonce, foundHash := lc.Engine.Mine(genesis)
	genesis.Nonce = foundNonce
	genesis.Hash = foundHash
	genesis.Reward = exactReward

	lc.Chain = append(lc.Chain, genesis)
	lc.Storage.SaveBlock(0, genesis)
	
	fmt.Printf("[GENESIS] Block 0 Loaded/Spawned with message: '%s'\n", pszTimestamp)
	fmt.Printf("[GENESIS PARAMS] Timestamp: %d | Nonce: %d | Bits: %d | MaxSupply: %.8f | HalvingInterval: %d | Hash: %x\n", genesisTimestamp, genesis.Nonce, genesis.Bits, formatCoin(genesis.MaxSupply), genesis.HalvingInterval, genesis.Hash)
}

// AddToMempool validates and inserts a transaction payload into the pending mempool queue with Fee Market priority sorting.
func (lc *LedgerCore) AddToMempool(tx *core.Transfer) bool {
	params := consensus.DefaultConsensus()
	requiredPrefix := params.AddressPrefix
	if !strings.HasPrefix(tx.Recipient, requiredPrefix) {
		fmt.Printf("[MEMPOOL REJECTION] Invalid recipient address prefix: '%s'. Network strictly requires '%s'\n", tx.Recipient, requiredPrefix)
		return false
	}

	lc.Mu.Lock()
	defer lc.Mu.Unlock()

	if !tx.Verify() {
		fmt.Println("[MEMPOOL] Invalid transaction cryptographic signature!")
		return false
	}

	sender := crypto.PubkeyToAddress(tx.SenderPubKey)
	acc, exists := lc.State[sender]
	
	if !exists {
		lc.State[sender] = &AccountState{Balance: 0, Nonce: tx.Nonce}
		acc = lc.State[sender]
	}

	if tx.Nonce != acc.Nonce {
		acc.Nonce = tx.Nonce
	}

	lc.Mempool = append(lc.Mempool, tx)

	sort.Slice(lc.Mempool, func(i, j int) bool {
		return lc.Mempool[i].Fee > lc.Mempool[j].Fee
	})

	fmt.Printf("[MEMPOOL] Transaction successfully queued with Fee: %.8f (ID: %s...)\n", formatCoin(tx.Fee), tx.ComputeID()[:12])
	return true
}

// GetMempoolFeeStats calculates the total, highest, and average fee metrics from transactions residing within the mempool.
func (lc *LedgerCore) GetMempoolFeeStats() (int, uint64, float64) {
	lc.Mu.RLock()
	defer lc.Mu.RUnlock()

	count := len(lc.Mempool)
	if count == 0 {
		return 0, 0, 0
	}

	var totalFee uint64 = 0
	highestFee := lc.Mempool[0].Fee

	for _, tx := range lc.Mempool {
		totalFee += tx.Fee
	}

	avgFee := float64(totalFee) / float64(count)
	return count, highestFee, avgFee
}

// StartLiveWorker starts the background worker daemon to periodically mine blocks from pending mempool transactions or empty blocks.
func (lc *LedgerCore) StartLiveWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if len(lc.Mempool) > 0 {
					lc.MineBlock()
				}
			case <-lc.StopSignal:
				ticker.Stop()
				return
			}
		}
	}()
}

// MineBlock packages mempool transactions, executes proof-of-work mining, and appends the new block to the ledger.
func (lc *LedgerCore) MineBlock() {
	params := consensus.DefaultConsensus()
	if lc.MinerAddress != "SYSTEM_GENESIS" && !strings.HasPrefix(lc.MinerAddress, params.AddressPrefix) {
		panic(fmt.Sprintf("[CONSENSUS VIOLATION] Invalid miner address prefix! Expected prefix '%s', got '%s'", params.AddressPrefix, lc.MinerAddress))
	}

	lc.Mu.Lock()
	parent := lc.Chain[len(lc.Chain)-1]
	validTx := make([]*core.Transfer, 0)
	var feeTotal uint64 = 0

	if len(lc.Mempool) > 0 {
		var currentBlockBytes uint64 = 512

		for i := 0; i < len(lc.Mempool); i++ {
			tx := lc.Mempool[i]
			
			txBytes, err := json.Marshal(tx)
			txSize := uint64(len(txBytes))
			if err != nil || currentBlockBytes+txSize > consensus.MaxBlockSizeBytes {
				break
			}

			sender := crypto.PubkeyToAddress(tx.SenderPubKey)
			if _, ok := lc.State[sender]; !ok {
				lc.State[sender] = &AccountState{Balance: 0, Nonce: 0}
			}
			acc := lc.State[sender]

			totalCost := tx.Value + tx.Fee
			if acc.Balance < totalCost {
				fmt.Printf("[MINER REJECTION] Insufficient balance for sender %s (Has: %.8f, Needed: %.8f)\n", sender, formatCoin(acc.Balance), formatCoin(totalCost))
				continue
			}

			acc.Balance -= totalCost
			acc.Nonce++

			if _, ok := lc.State[tx.Recipient]; !ok {
				lc.State[tx.Recipient] = &AccountState{Balance: tx.Value, Nonce: 0}
			} else {
				lc.State[tx.Recipient].Balance += tx.Value
			}

			feeTotal += tx.Fee
			currentBlockBytes += txSize
			validTx = append(validTx, tx)
			fmt.Printf("[MINER] -> Processing Priority Tx: %.8f Coins to %s (Fee: %.8f)\n", formatCoin(tx.Value), tx.Recipient, formatCoin(tx.Fee))
		}
		
		if len(validTx) > 0 {
			lc.Mempool = lc.Mempool[len(validTx):]
		}
	}
	lc.Mu.Unlock()

	nextIndex := parent.Index + 1
	
	currentSupply := lc.GetTotalCirculatingSupply()
	exactReward := CalculateBlockReward(nextIndex)

	if currentSupply >= consensus.MaxSupply {
		exactReward = 0
		fmt.Println("[WARNING] MaxSupply hard cap reached! Block reward is now 0.")
	} else if currentSupply+exactReward > consensus.MaxSupply {
		exactReward = consensus.MaxSupply - currentSupply
	}

	currentTime := time.Now().Unix()
	
	if currentTime <= parent.Timestamp {
		currentTime = parent.Timestamp + 1
	}

	calculatedDiff := consensus.CalculateNextDifficulty(parent.Timestamp, currentTime, uint64(parent.Difficulty))
	lc.Engine.TargetDifficulty = uint32(calculatedDiff)

	newBlock := &core.LedgerBlock{
		Index:           nextIndex,
		Timestamp:       currentTime,
		PrevHash:        parent.Hash,
		Transfers:       validTx,
		Miner:           lc.MinerAddress,
		Difficulty:      lc.Engine.TargetDifficulty,
		Bits:            lc.Engine.Bits,
		Reward:          exactReward,
		MaxSupply:       consensus.MaxSupply,
		HalvingInterval: consensus.HalvingInterval,
	}

	fmt.Printf("[MINER] Mining Block #%d with %d transactions (Dynamic Difficulty: %d)...\n", newBlock.Index, len(validTx), newBlock.Difficulty)
	
	startTime := time.Now()
	nonce, hash := lc.Engine.Mine(newBlock)
	duration := time.Since(startTime)

	newBlock.Nonce = nonce
	newBlock.Hash = hash
	newBlock.Reward = exactReward

	if err := core.ValidateBlockConsensus(newBlock, parent, currentSupply); err != nil {
		panic(fmt.Sprintf("[CRITICAL CONSENSUS VIOLATION] Generated block failed strict validation: %v", err))
	}

	lc.Mu.Lock()
	lc.Chain = append(lc.Chain, newBlock)
	lc.Storage.SaveBlock(newBlock.Index, newBlock)

	totalMinerReward := newBlock.Reward + feeTotal
	if totalMinerReward > 0 {
		if _, ok := lc.State[lc.MinerAddress]; !ok {
			lc.State[lc.MinerAddress] = &AccountState{Balance: totalMinerReward, Nonce: 0}
		} else {
			lc.State[lc.MinerAddress].Balance += totalMinerReward
		}
	}
	lc.Mu.Unlock()

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("[SUCCESS] Block #%d Mined & Saved! (Reward: %.8f, Fee: %.8f, Nonce: %d, Time: %v)\n", newBlock.Index, formatCoin(newBlock.Reward), formatCoin(feeTotal), newBlock.Nonce, duration)
	fmt.Printf("[CHAIN] Total Blocks: %d | Circulating Supply: %.8f / %.8f\n", len(lc.Chain), formatCoin(lc.GetTotalCirculatingSupply()), formatCoin(consensus.MaxSupply))
	fmt.Println("--------------------------------------------------------------------------------")
}

// DeleteBlock removes a block entry from the database by its index.
func (lc *LedgerCore) DeleteBlock(index uint64) error {
	if lc.Storage == nil {
		return fmt.Errorf("storage database is uninitialized")
	}
	return lc.Storage.DeleteBlock(index)
}

// RollbackBlock removes the latest block from memory, storage, and reverts account state changes.
func (lc *LedgerCore) RollbackBlock() error {
	if len(lc.Chain) <= 1 {
		return fmt.Errorf("cannot rollback genesis block")
	}

	tip := lc.Chain[len(lc.Chain)-1]

	for _, tx := range tip.Transfers {
		lc.Mempool = append([]*core.Transfer{tx}, lc.Mempool...)
	}

	lc.Chain = lc.Chain[:len(lc.Chain)-1]

	if lc.Storage != nil {
		if err := lc.DeleteBlock(tip.Index); err != nil {
			return fmt.Errorf("failed to delete block from storage: %v", err)
		}
	}

	lc.State = make(map[string]*AccountState)
	for _, block := range lc.Chain {
		lc.RebuildState(block)
	}

	fmt.Printf("[LEDGER] Successfully rolled back block #%d\n", tip.Index)
	return nil
}

// GetBlockByHash retrieves a block from the chain matching the specified hash string.
func (lc *LedgerCore) GetBlockByHash(hashHex string) *core.LedgerBlock {
	for _, block := range lc.Chain {
		if hexHash := fmt.Sprintf("%x", block.Hash); hexHash == hashHex || block.Index == 0 && hashHex == "" {
			return block
		}
	}
	return nil
}

// GetLatestBlock returns the highest tip block in the active chain.
func (lc *LedgerCore) GetLatestBlock() *core.LedgerBlock {
	if len(lc.Chain) == 0 {
		return nil
	}
	return lc.Chain[len(lc.Chain)-1]
}

// AppendBlockDirectly appends an alternative valid block directly to the chain during a reorg process.
func (lc *LedgerCore) AppendBlockDirectly(block *core.LedgerBlock) error {
	parent := lc.GetLatestBlock()
	if parent != nil && string(block.PrevHash) != string(parent.Hash) {
		return fmt.Errorf("block previous hash does not match current chain tip")
	}

	currentSupply := lc.GetTotalCirculatingSupply()
	if err := core.ValidateBlockConsensus(block, parent, currentSupply); err != nil {
		return fmt.Errorf("consensus validation failed for alternative block: %v", err)
	}

	lc.Chain = append(lc.Chain, block)
	if lc.Storage != nil {
		lc.Storage.SaveBlock(block.Index, block)
	}
	lc.RebuildState(block)

	fmt.Printf("[LEDGER] Successfully appended alternative block #%d to active chain\n", block.Index)
	return nil
}
