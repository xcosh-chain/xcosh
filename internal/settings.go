package internal

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
)

// Config structure holds the parameter values required for node operation.
type Config struct {
	DataDir      string
	Port         string
	RPCPort      string
	RPCUser      string
	RPCPassword  string
	CookieString string
}

// LoadConfig reads configuration parameters from the specified data directory.
func LoadConfig(dataDir string) (*Config, error) {
	// Initialize default configuration values
	cfg := &Config{
		DataDir:     dataDir,
		Port:        ":19333",
		RPCPort:     "19332",
		RPCUser:     "",
		RPCPassword: "",
	}

	// Ensure that the target data directory exists with appropriate permissions
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return cfg, err
	}

	// Attempt to locate and open the configuration file
	configPath := filepath.Join(dataDir, "xcosh.conf")
	if file, err := os.Open(configPath); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			// Ignore empty lines and comment lines
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			// Split configuration line into key and value components
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}

			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])

			// Assign parsed configuration values to corresponding fields
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
	}

	// Define the path for the authentication cookie file
	cookiePath := filepath.Join(dataDir, ".cookie")
	
	// Generate credentials automatically if user credentials are not provided
	if cfg.RPCUser == "" && cfg.RPCPassword == "" {
		if _, err := os.Stat(cookiePath); os.IsNotExist(err) {
			randomBytes := make([]byte, 32)
			rand.Read(randomBytes)
			cookieData := "__cookie__:" + hex.EncodeToString(randomBytes)
			_ = os.WriteFile(cookiePath, []byte(cookieData), 0600)
		}
		
		// Read authentication credentials from the cookie file
		if cookieBytes, err := os.ReadFile(cookiePath); err == nil {
			cfg.CookieString = string(cookieBytes)
			parts := strings.SplitN(cfg.CookieString, ":", 2)
			if len(parts) == 2 {
				cfg.RPCUser = parts[0]
				cfg.RPCPassword = parts[1]
			}
		}
	}

	return cfg, nil
}
