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

package crypto

import (
	"encoding/hex"

	"xcosh/crypto/dilithium3"
	"xcosh/crypto/sha3"
	"github.com/cloudflare/circl/sign/dilithium/mode3"
	golangSha3 "golang.org/x/crypto/sha3"
)

// GenerateKey delegates post-quantum keypair creation to the dilithium3 module.
func GenerateKey() (*mode3.PublicKey, *mode3.PrivateKey, error) {
	return dilithium3.GenerateKey()
}

// Hash512 delegates SHA3-512 cryptographic hashing operations to the sha3 module.
func Hash512(data []byte) []byte {
	return sha3.Hash512(data)
}

// Hash256 computes a Keccak-256 cryptographic hash digest matching the Ethereum standard.
func Hash256(data []byte) []byte {
	d := golangSha3.NewLegacyKeccak256()
	d.Write(data)
	return d.Sum(nil)
}

// PubkeyToAddress derives a custom post-quantum network address string from a Dilithium public key bytes slice.
func PubkeyToAddress(pubBytes []byte) string {
	rawHex := hex.EncodeToString(pubBytes[:14])
	return "xcosh" + rawHex
}

// Sign delegates signature generation to the dilithium3 module.
func Sign(priv *mode3.PrivateKey, message []byte) ([]byte, error) {
	return dilithium3.Sign(priv, message)
}

// Verify delegates signature verification to the dilithium3 module.
func Verify(pub *mode3.PublicKey, message, sig []byte) bool {
	return dilithium3.Verify(pub, message, sig)
}
