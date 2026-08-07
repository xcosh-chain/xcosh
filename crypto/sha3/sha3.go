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

package sha3

import (
	"golang.org/x/crypto/sha3"
)

// Hash512 computes a standard 512-bit SHA3 hash of the given data payload.
func Hash512(data []byte) []byte {
	hasher := sha3.New512()
	hasher.Write(data)
	return hasher.Sum(nil)
}

// Hash256 computes a Keccak-256 cryptographic hash of the given data payload.
func Hash256(data []byte) []byte {
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(data)
	return hasher.Sum(nil)
}
