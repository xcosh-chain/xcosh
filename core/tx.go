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
	"encoding/hex"
	"fmt"

	"xcosh/crypto"
	"github.com/cloudflare/circl/sign/dilithium/mode3"
	"golang.org/x/crypto/sha3"
)

// Transfer represents a state-transition transaction payload containing sender public key, recipient address, transfer values, nonce, and post-quantum signature.
type Transfer struct {
	SenderPubKey []byte `json:"sender_pub_key"`
	Recipient    string `json:"recipient"`
	Value        uint64 `json:"value"`
	Fee          uint64 `json:"fee"`
	Nonce        uint64 `json:"nonce"`
	Signature    []byte `json:"signature"`
}

// NewTransfer constructs a new Transfer transaction instance, computes its hash payload, and signs it using the Dilithium private key.
func NewTransfer(priv *mode3.PrivateKey, pub []byte, recipient string, value, fee, nonce uint64) *Transfer {
	// Initialize a new Transfer transaction structure with the provided parameters.
	tx := &Transfer{
		SenderPubKey: pub,
		Recipient:    recipient,
		Value:        value,
		Fee:          fee,
		Nonce:        nonce,
	}

	// Generate a post-quantum cryptographic signature over the canonical transaction payload bytes.
	sig, err := crypto.Sign(priv, tx.PayloadBytes())
	if err != nil {
		panic(err)
	}
	tx.Signature = sig
	// Return a pointer to the fully signed transfer transaction instance.
	return tx
}

// PayloadBytes serializes the primary transactional parameters into a canonical byte slice representation for signing and verification.
func (tx *Transfer) PayloadBytes() []byte {
	// Format and return the core transactional attributes as a standardized byte slice.
	return []byte(fmt.Sprintf("%s-%d-%d-%d", tx.Recipient, tx.Value, tx.Fee, tx.Nonce))
}

// Verify validates the post-quantum digital signature attached to the transaction against the sender's public key.
func (tx *Transfer) Verify() bool {
	var pub mode3.PublicKey
	// Unmarshal the raw sender public key bytes into a functional Dilithium public key instance.
	if err := pub.UnmarshalBinary(tx.SenderPubKey); err != nil {
		return false
	}
	// Verify the attached post-quantum cryptographic signature against the transaction payload bytes.
	return crypto.Verify(&pub, tx.PayloadBytes(), tx.Signature)
}

// ComputeID generates a unique cryptographic hash identifier string for the transaction instance using Keccak-256.
func (tx *Transfer) ComputeID() string {
	// Hash the combination of the sender public key and payload bytes using Keccak-256 to produce a unique transaction ID.
	d := sha3.NewLegacyKeccak256()
	d.Write(append(tx.SenderPubKey, tx.PayloadBytes()...))
	hash := d.Sum(nil)
	// Encode the resulting hash digest into a hexadecimal string representation.
	return hex.EncodeToString(hash)
}
