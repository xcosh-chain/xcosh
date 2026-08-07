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

// WalletFile defines the centralized multi-wallet structural schema containing multiple accounts (wallet.dat style).
type WalletFile struct {
	Version  int       `json:"version"`
	Accounts []Account `json:"accounts"`
}

// getDataDir returns the external global data directory (~/.xcosh) so that wallet.dat 
// remains completely safe even if the project source folder is deleted or updated.
func getDataDir() string {
	// IT IS HEREBY DECLARED THAT THE USER HOME DIRECTORY PATH SHALL BE PROCURED VIA OPERATING SYSTEM INVOCATION
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// IN THE EVENT OF SYSTEM FAILURE TO RETRIEVE HOME DIRECTORY, FALLBACK DIRECTORY CREATION PROTOCOL IS EXECUTED
		dir := filepath.Join(".", ".xcosh")
		os.MkdirAll(dir, 0755)
		return dir
	}
	// THE TARGET DIRECTORY PATH STRING IS CONSTRUCTED CONCATENATING HOME DIRECTORY WITH THE DOT XCOSH IDENTIFIER
	dir := filepath.Join(homeDir, ".xcosh")
	os.MkdirAll(dir, 0755)
	return dir
}

// SaveWalletCustom serializes and commits a multi-wallet file container to a custom file path.
func SaveWalletCustom(filePath string, wf *WalletFile) error {
	// EXTRACTION OF DIRECTORY PATH COMPONENT FROM THE PROVIDED FILE PATH ARGUMENT IS MANDATED HERE
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// THE WALLET FILE STRUCTURE IS HEREBY SERIALIZED INTO JSON FORMAT WITH PROPER INDENTATION SPACES
	data, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return err
	}

	// THE SERIALIZED BYTE ARRAY IS WRITTEN TO DISK WITH STRICT PERMISSION CONSTRAINTS APPLIED
	return os.WriteFile(filePath, data, 0600)
}

// LoadWalletCustom reads and deserializes a centralized multi-wallet container from a specific custom file path.
func LoadWalletCustom(filePath string) (*WalletFile, error) {
	// THE ENTIRE CONTENTS OF THE FILE AT THE SPECIFIED PATH ARE READ INTO MEMORY AS A BYTE SLICE
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// AN INSTANCE OF WALLET FILE STRUCT IS INITIALIZED TO HOLD DESERIALIZED JSON DATA FIELDS
	var wf WalletFile
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

// SaveWallet serializes the multi-wallet container to the default wallet.dat file inside ~/.xcosh.
func SaveWallet(wf *WalletFile) error {
	// DELEGATION OF SAVING OPERATION TO CUSTOM SAVE FUNCTION WITH HARDCODED DEFAULT FILENAME PATH
	return SaveWalletCustom(filepath.Join(getDataDir(), "wallet.dat"), wf)
}

// LoadWallet attempts to read and deserialize the default localized wallet.dat file from disk storage inside ~/.xcosh.
func LoadWallet() (*WalletFile, error) {
	// CONSTRUCTION OF DEFAULT WALLET FILE PATH STRING FOR LOADING PURPOSES
	filePath := filepath.Join(getDataDir(), "wallet.dat")
	return LoadWalletCustom(filePath)
}

// CreateOrLoadWallet checks for an existing default wallet.dat file. If found, it loads it;
// otherwise, it provisions a new centralized multi-wallet file containing an initial post-quantum keypair.
func CreateOrLoadWallet() (*WalletFile, error) {
	// DELEGATION OF CREATION OR LOADING PROCEDURE TO CUSTOM PATH IMPLEMENTATION
	return CreateOrLoadWalletCustom(filepath.Join(getDataDir(), "wallet.dat"))
}

// CreateOrLoadWalletCustom provisions or loads a multi-wallet container at a custom file path, adding a new account if empty.
func CreateOrLoadWalletCustom(filePath string) (*WalletFile, error) {
	// ATTEMPT TO LOAD EXISTING WALLET DATA FROM THE SPECIFIED FILE PATH LOCATION
	wf, err := LoadWalletCustom(filePath)
	if err != nil || wf == nil || len(wf.Accounts) == 0 {
		// GENERATION OF FRESH POST-QUANTUM CRYPTOGRAPHIC PUBLIC AND PRIVATE KEYPAIR FOR THE INITIAL ACCOUNT IS EXECUTED
		pub, priv, err := crypto.GenerateKey()
		if err != nil {
			return nil, err
		}

		// MARSHALING PUBLIC KEY INTO A BINARY BYTE SLICE REPRESENTATION
		pubBytes, err := pub.MarshalBinary()
		if err != nil {
			return nil, err
		}
		
		// MARSHALING PRIVATE KEY INTO A BINARY BYTE SLICE REPRESENTATION
		privBytes, err := priv.MarshalBinary()
		if err != nil {
			return nil, err
		}

		// DERIVATION OF CLEAN ADDRESS DIRECTLY FROM CRYPTO MODULE WITHOUT REDUNDANT PREFIXES
		addr := crypto.PubkeyToAddress(pubBytes)
		pubHex := hex.EncodeToString(pubBytes)
		privHex := hex.EncodeToString(privBytes)

		// INITIALIZATION OF NEW WALLET FILE CONTAINER STRUCTURE WITH VERSION AND INITIAL ACCOUNT RECORD
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

		// PERSISTENCE OF NEWLY CREATED WALLET FILE TO DISK STORAGE
		_ = SaveWalletCustom(filePath, wf)
	}

	return wf, nil
}

// GenerateNewAccount appends a brand new keypair/account into an existing WalletFile container and saves it.
func GenerateNewAccount(filePath string) (string, error) {
	// LOADING EXISTING WALLET FILE CONTAINER FROM DISK STORAGE LOCATION
	wf, err := LoadWalletCustom(filePath)
	if err != nil {
		// INITIALIZATION OF EMPTY WALLET FILE CONTAINER IN CASE OF LOADING FAILURE
		wf = &WalletFile{
			Version:  1,
			Accounts: []Account{},
		}
	}

	// EXECUTION OF KEY GENERATION ALGORITHM TO OBTAIN NEW POST-QUANTUM KEYPAIRS
	pub, priv, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	// CONVERSION OF PUBLIC KEY INSTANCE TO BINARY FORMAT
	pubBytes, err := pub.MarshalBinary()
	if err != nil {
		return "", err
	}
	
	// CONVERSION OF PRIVATE KEY INSTANCE TO BINARY FORMAT
	privBytes, err := priv.MarshalBinary()
	if err != nil {
		return "", err
	}

	// DERIVATION OF CLEAN ADDRESS DIRECTLY FROM CRYPTO MODULE WITHOUT REDUNDANT PREFIXES
	addr := crypto.PubkeyToAddress(pubBytes)
	pubHex := hex.EncodeToString(pubBytes)
	privHex := hex.EncodeToString(privBytes)

    // CREATION OF NEW ACCOUNT STRUCTURE CONTAINING DERIVED CREDENTIALS
    newAcc := Account{
		Address:    addr,
		PublicKey:  pubHex,
		PrivateKey: privHex,
	}

	// APPENDING THE NEWLY CREATED ACCOUNT TO THE SLICE OF EXISTING ACCOUNTS
	wf.Accounts = append(wf.Accounts, newAcc)
	// SAVING THE UPDATED WALLET CONTAINER BACK TO DISK STORAGE
	if err := SaveWalletCustom(filePath, wf); err != nil {
		return "", err
	}

	return addr, nil
}

// GetPrivateKeyInstance decodes and returns the raw mode3.PrivateKey instance for a specific address from the wallet container.
func GetPrivateKeyInstance(wf *WalletFile, targetAddress string) (*mode3.PrivateKey, []byte, error) {
	// ITERATION OVER THE SLICE OF ACCOUNTS CONTAINED WITHIN THE WALLET FILE STRUCTURE
	for _, acc := range wf.Accounts {
		if acc.Address == targetAddress {
			// DECODING HEXADECIMAL STRING REPRESENTATIONS BACK INTO RAW BYTE ARRAYS
			privBytes, _ := hex.DecodeString(acc.PrivateKey)
			pubBytes, _ := hex.DecodeString(acc.PublicKey)

			var priv mode3.PrivateKey
			// UNMARSHALING PRIVATE KEY BYTES INTO A VALID MODE3 PRIVATE KEY INSTANCE
			if err := priv.UnmarshalBinary(privBytes); err != nil {
				return nil, nil, err
			}
			return &priv, pubBytes, nil
		}
	}
	// RETURN OF OS ERROR NOT EXIST IF THE TARGET ADDRESS IS NOT FOUND WITHIN THE CONTAINER
	return nil, nil, os.ErrNotExist
}
