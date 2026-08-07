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

package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"xcosh/internal/cli"
	"xcosh/internal/daemon"

	_ "github.com/cloudflare/circl/sign/dilithium/mode3"
)

// main serves as the primary router entry point for the command-line interface application,
// parsing operational arguments and dispatching execution flows to dedicated module handlers.
func main() {
	// Initialize distinct command-line flag sets for various administrative and operational subcommands.
	walletCreateCmd := flag.NewFlagSet("create", flag.ExitOnError)
	balanceCmd := flag.NewFlagSet("balance", flag.ExitOnError)
	supplyCmd := flag.NewFlagSet("supply", flag.ExitOnError)
	sendCmd := flag.NewFlagSet("send", flag.ExitOnError)
	nodeCmd := flag.NewFlagSet("node", flag.ExitOnError)
	explorerCmd := flag.NewFlagSet("explorer", flag.ExitOnError)
	mineCmd := flag.NewFlagSet("mine", flag.ExitOnError)
	miningCmd := flag.NewFlagSet("mining", flag.ExitOnError)
	peersCmd := flag.NewFlagSet("peers", flag.ExitOnError)
	feesCmd := flag.NewFlagSet("fees", flag.ExitOnError)
	getBlockHashCmd := flag.NewFlagSet("getblockhash", flag.ExitOnError)
	getBlockCmd := flag.NewFlagSet("getblock", flag.ExitOnError)
	uptimeCmd := flag.NewFlagSet("uptime", flag.ExitOnError)
	getNetTotalsCmd := flag.NewFlagSet("getnettotals", flag.ExitOnError)
	addNodeCmd := flag.NewFlagSet("addnode", flag.ExitOnError)
	blockSizeCmd := flag.NewFlagSet("blocksize", flag.ExitOnError)

	// Define specific parameter bindings for individual command flags.
	walletLabel := walletCreateCmd.String("label", "Default Account", "Label description for the new multi-wallet account")

	sendRecipient := sendCmd.String("to", "", "Recipient destination address")
	sendAmountStr := sendCmd.String("amount", "0", "Transfer value amount in coins (supports small decimals or large numbers)")
	sendFee := sendCmd.Uint64("fee", 2, "Transaction fee")
	sendSenderAddr := sendCmd.String("from", "", "Specific sender account address within wallet.dat")

	nodePort := nodeCmd.String("port", ":19333", "P2P listening port for the node")
	nodeConnect := nodeCmd.String("connect", "", "Peer address to connect (e.g., localhost:19333)")

	mineBlocks := mineCmd.Int("blocks", 1, "Number of blocks to generate")
	mineAddress := mineCmd.String("address", "", "Target destination address for block reward (generatetoaddress style)")

	addNodeTarget := addNodeCmd.String("to", "", "Target peer address to add (e.g., localhost:19333)")

	// Validate whether adequate command-line arguments have been provided by the executing operator.
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	// Check if the command is meant to be handled via JSON-RPC client forwarding.
	// Common RPC methods like getblockcount, getconnectioncount, getinfo, etc. can be passed directly.
	switch subcommand {
	case "create":
		walletCreateCmd.Parse(os.Args[2:])
		cli.HandleCreateWalletAccount(*walletLabel)
	case "balance":
		balanceCmd.Parse(os.Args[2:])
		cli.HandleCheckBalance()
	case "supply":
		supplyCmd.Parse(os.Args[2:])
		cli.HandleCheckSupply()
	case "send":
		sendCmd.Parse(os.Args[2:])
		var amountInUnits uint64
		if val, err := strconv.ParseFloat(*sendAmountStr, 64); err == nil {
			amountInUnits = uint64(val * 100000000)
		} else {
			amountInUnits = 0
		}
		cli.HandleSendTx(*sendRecipient, amountInUnits, *sendFee, *sendSenderAddr)
	case "node":
		nodeCmd.Parse(os.Args[2:])
		daemon.RunNodeDaemon(*nodePort, *nodeConnect)
	case "explorer":
		explorerCmd.Parse(os.Args[2:])
		cli.HandleExploreBlockchain()
	case "mine":
		mineCmd.Parse(os.Args[2:])
		cli.HandleManualMine(*mineBlocks, *mineAddress)
	case "mining":
		if len(os.Args) < 3 {
			fmt.Println("Usage: ./xcosh mining <target_address>")
			os.Exit(1)
		}
		miningCmd.Parse(os.Args[3:])
		cli.HandleManualMine(1, os.Args[2])
	case "addnode":
		if len(os.Args) < 3 {
			fmt.Println("Usage: ./xcosh addnode <host:port>")
			os.Exit(1)
		}
		addNodeCmd.Parse(os.Args[2:])
		cli.HandleAddNode(*addNodeTarget)
	case "peers":
		peersCmd.Parse(os.Args[2:])
		cli.HandleCheckPeers()
	case "fees":
		feesCmd.Parse(os.Args[2:])
		cli.HandleCheckFees()
	case "uptime":
		uptimeCmd.Parse(os.Args[2:])
		cli.HandleCheckUptime()
	case "getnettotals":
		getNetTotalsCmd.Parse(os.Args[2:])
		cli.HandleGetNetTotals()
	case "blocksize":
		blockSizeCmd.Parse(os.Args[2:])
		cli.HandleCheckBlockSize()
	case "getblockhash":
		if len(os.Args) < 3 {
			fmt.Println("Usage: ./xcosh getblockhash <block_index>")
			os.Exit(1)
		}
		getBlockHashCmd.Parse(os.Args[2:])
		cli.HandleGetBlockHash(os.Args[2])
	case "getblock":
		if len(os.Args) < 3 {
			fmt.Println("Usage: ./xcosh getblock <block_hash>")
			os.Exit(1)
		}
		getBlockCmd.Parse(os.Args[2:])
		cli.HandleGetBlock(os.Args[2])
	default:
		// Forward any other subcommand (such as getblockcount, getinfo, getconnectioncount)
		// directly to the running daemon via the JSON-RPC client handler.
		var params []interface{}
		for _, arg := range os.Args[2:] {
			params = append(params, arg)
		}
		cli.HandleRPCClient(subcommand, params)
	}
}

// printUsage outputs the standard command-line manual instructions and available command options to standard output.
func printUsage() {
	fmt.Println("================================================================================")
	fmt.Println(" XCOSH BLOCKCHAIN CLI MANAGER (MULTI-WALLET ARCHITECTURE)")
	fmt.Println("================================================================================")
	fmt.Println("Available commands:")
	fmt.Println("  ./xcosh create [-label <account_label>]")
	fmt.Println("  ./xcosh balance")
	fmt.Println("  ./xcosh supply")
	fmt.Println("  ./xcosh send -from <addr> -amount <val> [-fee <val>] [-to <sender_addr>]")
	fmt.Println("  ./xcosh node [--port :port] [--connect host:port]")
	fmt.Println("  ./xcosh addnode <host:port>")
	fmt.Println("  ./xcosh mine [-blocks <num>] [-address <addr>]")
	fmt.Println("  ./xcosh mining <target_address>")
	fmt.Println("  ./xcosh explorer")
	fmt.Println("  ./xcosh peers")
	fmt.Println("  ./xcosh fees")
	fmt.Println("  ./xcosh uptime")
	fmt.Println("  ./xcosh getnettotals")
	fmt.Println("  ./xcosh blocksize")
	fmt.Println("  ./xcosh getblockhash <index>")
	fmt.Println("  ./xcosh getblock <hash>")
	fmt.Println("  ./xcosh getblockcount (RPC)")
	fmt.Println("  ./xcosh getinfo (RPC)")
	fmt.Println("  ./xcosh getconnectioncount (RPC)")
	fmt.Println("================================================================================")
}
