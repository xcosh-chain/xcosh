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

package internal

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// Config holds the node configuration parameters loaded from xcosh.conf.
type Config struct {
	Port        string
	RPCPort     string
	RPCUser     string
	RPCPassword string
}

// LoadConfig reads and parses the xcosh.conf configuration file from the specified data directory.
// It returns a pointer to a Config struct populated with the parsed values, 
// or falls back to default settings if the configuration file does not exist.
func LoadConfig(dataDir string) (*Config, error) {
	// Initialize default configuration matching standard Bitcoin Core behavior
	cfg := &Config{
		Port:        ":19333",
		RPCPort:     "19332",
		RPCUser:     "",
		RPCPassword: "",
	}

	// Construct the full path to the configuration file
	configPath := filepath.Join(dataDir, "xcosh.conf")
	file, err := os.Open(configPath)
	if err != nil {
		// If the configuration file is missing, return default values safely without returning an error
		return cfg, nil
	}
	defer file.Close()

	// Scan the configuration file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comment lines starting with '#'
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split the line into key-value pairs separated by '='
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		// Map configuration keys to their respective struct fields
		switch key {
		case "port":
			if !strings.HasPrefix(val, ":") {
				cfg.Port = ":" + val
			} else {
				cfg.Port = val
			}
		case "rpcport":
			cfg.RPCPort = val
		case "rpcuser":
			cfg.RPCUser = val
		case "rpcpassword":
			cfg.RPCPassword = val
		}
	}

	return cfg, nil
}
