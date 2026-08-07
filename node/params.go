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

// Financial and decimal parameter constants for Xcosh (8 decimal precision).
const (
	CoinUnit       = 100000000 // 1 Xcosh = 100,000,000 smallest units
	InitialAirdrop = 10000 * CoinUnit
)

// ToDecimal converts the smallest integer unit value into an 8-decimal float format.
func ToDecimal(amount uint64) float64 {
	// Divide the raw integer smallest unit value by the coin unit multiplier to obtain the standard float representation.
	return float64(amount) / float64(CoinUnit)
}

// ToUnits converts a standard float coin value into the smallest integer unit representation.
func ToUnits(amount float64) uint64 {
	// Multiply the standard coin float value by the coin unit multiplier to convert it into raw integer base units.
	return uint64(amount * float64(CoinUnit))
}
