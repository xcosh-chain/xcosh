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

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"xcosh/internal"
)

// HandleCheckBlockSize retrieves and prints the physical storage size of the blockchain database.
func HandleCheckBlockSize() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("[ERROR] Failed to get user home directory: %v\n", err)
		return
	}
	dbPath := filepath.Join(homeDir, ".xcosh")

	sizeBytes, err := internal.GetBlockChainStorageSize(dbPath)
	if err != nil {
		fmt.Printf("[ERROR] Failed to calculate blockchain storage size: %v\n", err)
		return
	}

	sizeMB := float64(sizeBytes) / (1024 * 1024)
	fmt.Println("================================================================================")
	fmt.Println(" XCOSH BLOCKCHAIN STORAGE SIZE")
	fmt.Println("================================================================================")
	fmt.Printf(" Physical Path : %s\n", dbPath)
	fmt.Printf(" Size in Bytes : %d bytes\n", sizeBytes)
	fmt.Printf(" Size in MB    : %.2f MB\n", sizeMB)
	fmt.Println("================================================================================")
}
