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

package rpc

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"time"

	"xcosh/core"
	"xcosh/internal"
	"xcosh/internal/cli"
	"xcosh/internal/p2p"
	"xcosh/node"
	"xcosh/storage/wallet"
)

// RPCRequest represents the incoming JSON-RPC request structure.
type RPCRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      interface{}   `json:"id"`
}

// RPCResponse represents the standard JSON-RPC response structure.
type RPCResponse struct {
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
	ID     interface{} `json:"id"`
}

// StartRPCServer starts the JSON-RPC HTTP server with Basic Authentication on the specified port.
func StartRPCServer(rpcPort string, ledger interface{}, cfg *internal.Config) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || cfg == nil || 
			subtle.ConstantTimeCompare([]byte(user), []byte(cfg.RPCUser)) != 1 || 
			subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.RPCPassword)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Xcosh RPC"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req RPCRequest
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response := RPCResponse{ID: req.ID}

		v := reflect.ValueOf(ledger)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		switch req.Method {
		case "getblockcount":
			count := 0
			if v.IsValid() && v.Kind() == reflect.Struct {
				chainField := v.FieldByName("Chain")
				if chainField.IsValid() {
					count = chainField.Len()
				}
			}
			response.Result = count

		case "getconnectioncount":
			response.Result = 1

		case "getbestblockhash":
			bestHash := ""
			if v.IsValid() && v.Kind() == reflect.Struct {
				chainField := v.FieldByName("Chain")
				if chainField.IsValid() && chainField.Len() > 0 {
					latestBlock := chainField.Index(chainField.Len() - 1)
					if latestBlock.Kind() == reflect.Ptr {
						latestBlock = latestBlock.Elem()
					}
					hashField := latestBlock.FieldByName("Hash")
					if hashField.IsValid() && hashField.Kind() == reflect.String {
						bestHash = hashField.String()
					}
				}
			}
			response.Result = bestHash

		case "getmininginfo":
			blocks := 0
			difficulty := 0.0
			if v.IsValid() && v.Kind() == reflect.Struct {
				chainField := v.FieldByName("Chain")
				if chainField.IsValid() {
					blocks = chainField.Len()
					if blocks > 0 {
						latestBlock := chainField.Index(blocks - 1)
						if latestBlock.Kind() == reflect.Ptr {
							latestBlock = latestBlock.Elem()
						}
						diffField := latestBlock.FieldByName("Difficulty")
						if diffField.IsValid() {
							switch diffField.Kind() {
							case reflect.Float32, reflect.Float64:
								difficulty = diffField.Float()
							case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
								difficulty = float64(diffField.Int())
							case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
								difficulty = float64(diffField.Uint())
							}
						}
					}
				}
			}
			response.Result = map[string]interface{}{
				"blocks":        blocks,
				"difficulty":    difficulty,
				"networkhashps": 0,
				"pooledtx":      0,
				"testnet":       false,
				"chain":         "main",
			}

		case "getinfo":
			blocks := 0
			difficulty := 0.0

			if v.IsValid() && v.Kind() == reflect.Struct {
				chainField := v.FieldByName("Chain")
				if chainField.IsValid() {
					blocks = chainField.Len()
					
					if blocks > 0 {
						latestBlock := chainField.Index(blocks - 1)
						if latestBlock.Kind() == reflect.Ptr {
							latestBlock = latestBlock.Elem()
						}
						
						diffField := latestBlock.FieldByName("Difficulty")
						if diffField.IsValid() {
							switch diffField.Kind() {
							case reflect.Float32, reflect.Float64:
								difficulty = diffField.Float()
							case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
								difficulty = float64(diffField.Int())
							case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
								difficulty = float64(diffField.Uint())
							}
						}
					}
				}
			}

			totalBalance := 0.0
			walletPath := filepathJoinWallet(cfg)
			if wf, err := wallet.LoadWalletCustom(walletPath); err == nil && wf != nil {
				if v.IsValid() && v.Kind() == reflect.Struct {
					stateField := v.FieldByName("State")
					if stateField.IsValid() && stateField.Kind() == reflect.Map {
						for _, acc := range wf.Accounts {
							if val := stateField.MapIndex(reflect.ValueOf(acc.Address)); val.IsValid() {
								accStruct := val.Elem()
								if accStruct.IsValid() && accStruct.Kind() == reflect.Struct {
									balField := accStruct.FieldByName("Balance")
									if balField.IsValid() {
										rawBal := balField.Uint()
										totalBalance += node.ToDecimal(rawBal)
									}
								}
							}
						}
					}
				}
			}

			response.Result = map[string]interface{}{
				"version":         1010000,
				"protocolversion": 70015,
				"walletversion":   130000,
				"balance":         totalBalance,
				"blocks":          blocks,
				"timeoffset":      0,
				"connections":     1,
				"proxy":           "",
				"difficulty":      difficulty,
				"testnet":         false,
				"keypoololdest":   time.Now().Unix() - 86400,
				"keypoolsize":     100,
				"paytxfee":        0.00001000,
				"relayfee":        0.00000010,
				"errors":          "",
			}

		case "generate":
			blocksCount := 1
			targetAddr := "SYSTEM_MINER"

			if len(req.Params) > 0 {
				if bc, ok := req.Params[0].(float64); ok {
					blocksCount = int(bc)
				}
			}
			if len(req.Params) > 1 {
				if ta, ok := req.Params[1].(string); ok && ta != "" {
					targetAddr = ta
				}
			}

			minedHashes := []string{}
			if v.IsValid() {
				mineMethod := reflect.ValueOf(ledger).MethodByName("MineBlock")
				if mineMethod.IsValid() {
					for i := 0; i < blocksCount; i++ {
						mineMethod.Call(nil)
						minedHashes = append(minedHashes, "Block mined successfully")
					}
				}
			}
			cli.SaveMempoolToDisk([]*core.Transfer{})

			response.Result = map[string]interface{}{
				"status":  "success",
				"mined":   blocksCount,
				"target":  targetAddr,
				"details": minedHashes,
			}

		case "addnode":
			if len(req.Params) > 0 {
				if peerAddr, ok := req.Params[0].(string); ok {
					home, err := os.UserHomeDir()
					dataDir := "."
					if err == nil {
						dataDir = home + "/.xcosh"
					}
					
					am := p2p.NewAddrManager(dataDir)
					
					if host, portStr, err := net.SplitHostPort(peerAddr); err == nil {
						var port int
						fmt.Sscanf(portStr, "%d", &port)
						am.AddAddress(host, port)
					}

					response.Result = fmt.Sprintf("Successfully added peer to addrman: %s", peerAddr)
				} else {
					response.Error = map[string]interface{}{
						"code":    -32602,
						"message": "Invalid params: peer address must be a string",
					}
				}
			} else {
				response.Error = map[string]interface{}{
					"code":    -32602,
					"message": "Missing parameter: peer address required",
				}
			}

		case "stop":
			response.Result = "Xcosh server stopping..."
			go func() {
				time.Sleep(1 * time.Second)
				os.Exit(0)
			}()

		default:
			response.Error = map[string]interface{}{
				"code":    -32601,
				"message": "Method not found",
			}
		}

		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%s", rpcPort)
	fmt.Printf("[RPC] JSON-RPC server listening (Auth Enabled) on port %s\n", rpcPort)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Printf("[RPC] Server error: %v\n", err)
		}
	}()
}

func filepathJoinWallet(cfg *internal.Config) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "wallet.dat"
	}
	return home + "/.xcosh/wallet.dat"
}
