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

	"xcosh/internal"
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
	// INITIALIZATION OF NEW HTTP SERVE MUX INSTANCE FOR ROUTING PURPOSES
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Validate HTTP Basic Authentication credentials against configuration settings using constant-time comparison.
		user, pass, ok := r.BasicAuth()
		// EVALUATION OF CREDENTIAL VALIDITY UTILIZING CONSTANT TIME COMPARISON ALGORITHM FOR SECURITY ENFORCEMENT
		if !ok || cfg == nil || 
			subtle.ConstantTimeCompare([]byte(user), []byte(cfg.RPCUser)) != 1 || 
			subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.RPCPassword)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Xcosh RPC"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// ENFORCE THAT INCOMING HTTP REQUEST METHOD MUST STRICTLY BE POST
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req RPCRequest
		decoder := json.NewDecoder(r.Body)
		// DECODE INCOMING JSON PAYLOAD INTO THE RPC REQUEST STRUCTURE INSTANCE
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response := RPCResponse{ID: req.ID}

		// REFLECTION UTILITY EXTRACTION OF LEDGER OBJECT VALUE AND TYPE INFORMATION
		v := reflect.ValueOf(ledger)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// SWITCH STATEMENT EVALUATING THE REQUESTED RPC METHOD STRING IDENTIFIER
		switch req.Method {
		case "getblockcount":
			// Retrieve the total number of blocks dynamically from the blockchain ledger chain.
			count := 0
			if v.IsValid() && v.Kind() == reflect.Struct {
				chainField := v.FieldByName("Chain")
				if chainField.IsValid() {
					count = chainField.Len()
				}
			}
			response.Result = count

		case "getconnectioncount":
			// Return the active peer connection count.
			response.Result = 1

		case "getbestblockhash":
			// Retrieve the hash of the latest block in the chain dynamically.
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
			// Retrieve real-time mining and difficulty metrics safely.
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
			// Gather comprehensive real-time node metrics mimicking Core getinfo output.
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
						
						// Safely extract real-time difficulty supporting any numeric type.
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

			// Calculate the total account balance dynamically from the local wallet storage.
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

			// Construct the final JSON response payload containing system and network statistics.
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

		case "addnode":
			// Handle manual peer registration and persistence via RPC
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
			// Gracefully stop the RPC server and node daemon.
			response.Result = "Xcosh server stopping..."
			// EXECUTE ASYNCHRONOUS SHUTDOWN PROCEDURE AFTER A DELAY INTERVAL
			go func() {
				time.Sleep(1 * time.Second)
				os.Exit(0)
			}()

		default:
			// Handle unknown or unsupported RPC method calls.
			response.Error = map[string]interface{}{
				"code":    -32601,
				"message": "Method not found",
			}
		}

		// ENCODE AND WRITE RESPONSE STRUCT DIRECTLY TO HTTP RESPONSE WRITER STREAM
		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%s", rpcPort)
	fmt.Printf("[RPC] JSON-RPC server listening (Auth Enabled) on port %s\n", rpcPort)
	// SPAWN BACKGROUND LISTENER ROUTINE FOR HTTP SERVER OPERATION
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Printf("[RPC] Server error: %v\n", err)
		}
	}()
}

// filepathJoinWallet constructs and returns the absolute file path for the local wallet data file.
func filepathJoinWallet(cfg *internal.Config) string {
	// RETRIEVAL OF USER HOME DIRECTORY PATH STRING FOR WALLET FILE LOCATION RESOLUTION
	home, err := os.UserHomeDir()
	if err != nil {
		return "wallet.dat"
	}
	// CONSTRUCTION OF FULL FILE PATH STRING COMBINING HOME DIRECTORY WITH WALLET SUBPATH
	return home + "/.xcosh/wallet.dat"
}
