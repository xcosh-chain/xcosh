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

package wallet

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"

	"xcosh/crypto"
	"github.com/cloudflare/circl/sign/dilithium/mode3"
)

// Account defines the structural schema for a single cryptographic keypair within the multi-wallet container.
type Account struct {
	Address    string `json:"address"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

// WalletFile defines the centralized multi-wallet structural schema containing multiple accounts (Bitcoin-like wallet.dat style).
type WalletFile struct {
	Version  int       `json:"version"`
	Accounts []Account `json:"accounts"`
}

// getDataDir returns the external global data directory (~/.xcosh) so that wallet.dat 
// remains completely safe even if the project source folder is deleted or updated.
func getDataDir() string {
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

// SaveWalletCustom serializes and commits a multi-wallet file container to a custom file path.
func SaveWalletCustom(filePath string, wf *WalletFile) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0600)
}

// LoadWalletCustom reads and deserializes a centralized multi-wallet container from a specific custom file path.
func LoadWalletCustom(filePath string) (*WalletFile, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var wf WalletFile
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

// SaveWallet serializes the multi-wallet container to the default wallet.dat file inside ~/.xcosh.
func SaveWallet(wf *WalletFile) error {
	return SaveWalletCustom(filepath.Join(getDataDir(), "wallet.dat"), wf)
}

// LoadWallet attempts to read and deserialize the default localized wallet.dat file from disk storage inside ~/.xcosh.
func LoadWallet() (*WalletFile, error) {
	filePath := filepath.Join(getDataDir(), "wallet.dat")
	return LoadWalletCustom(filePath)
}

// CreateOrLoadWallet checks for an existing default wallet.dat file. If found, it loads it;
// otherwise, it provisions a new centralized multi-wallet file containing an initial post-quantum keypair.
func CreateOrLoadWallet() (*WalletFile, error) {
	return CreateOrLoadWalletCustom(filepath.Join(getDataDir(), "wallet.dat"))
}

// CreateOrLoadWalletCustom provisions or loads a multi-wallet container at a custom file path, adding a new account if empty.
func CreateOrLoadWalletCustom(filePath string) (*WalletFile, error) {
	wf, err := LoadWalletCustom(filePath)
	if err != nil || wf == nil || len(wf.Accounts) == 0 {
		// Generate fresh post-quantum cryptographic public and private keypair for the initial account
		pub, priv, err := crypto.GenerateKey()
		if err != nil {
			return nil, err
		}

		pubBytes, err := pub.MarshalBinary()
		if err != nil {
			return nil, err
		}
		
		privBytes, err := priv.MarshalBinary()
		if err != nil {
			return nil, err
		}

		// Derive clean address directly from crypto module without redundant prefixes
		addr := crypto.PubkeyToAddress(pubBytes)
		pubHex := hex.EncodeToString(pubBytes)
		privHex := hex.EncodeToString(privBytes)

		wf = &WalletFile{
			Version: 1,
			Accounts: []Account{
				{
					Address:    addr,
					PublicKey:  pubHex,
					PrivateKey: privHex,
				},
			},
		}

		_ = SaveWalletCustom(filePath, wf)
	}

	return wf, nil
}

// GenerateNewAccount appends a brand new keypair/account into an existing WalletFile container and saves it.
func GenerateNewAccount(filePath string) (string, error) {
	wf, err := LoadWalletCustom(filePath)
	if err != nil {
		wf = &WalletFile{
			Version:  1,
			Accounts: []Account{},
		}
	}

	pub, priv, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	pubBytes, err := pub.MarshalBinary()
	if err != nil {
		return "", err
	}
	
	privBytes, err := priv.MarshalBinary()
	if err != nil {
		return "", err
	}

	// Derive clean address directly from crypto module without redundant prefixes
	addr := crypto.PubkeyToAddress(pubBytes)
	pubHex := hex.EncodeToString(pubBytes)
	privHex := hex.EncodeToString(privBytes)

    newAcc := Account{
		Address:    addr,
		PublicKey:  pubHex,
		PrivateKey: privHex,
	}

	wf.Accounts = append(wf.Accounts, newAcc)
	if err := SaveWalletCustom(filePath, wf); err != nil {
		return "", err
	}

	return addr, nil
}

// GetPrivateKeyInstance decodes and returns the raw mode3.PrivateKey instance for a specific address from the wallet container.
func GetPrivateKeyInstance(wf *WalletFile, targetAddress string) (*mode3.PrivateKey, []byte, error) {
	for _, acc := range wf.Accounts {
		if acc.Address == targetAddress {
			privBytes, _ := hex.DecodeString(acc.PrivateKey)
			pubBytes, _ := hex.DecodeString(acc.PublicKey)

			var priv mode3.PrivateKey
			if err := priv.UnmarshalBinary(privBytes); err != nil {
				return nil, nil, err
			}
			return &priv, pubBytes, nil
		}
	}
	return nil, nil, os.ErrNotExist
}
