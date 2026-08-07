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

package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// UptimeRecord stores the timestamp data representing when the node instance was initially booted.
type UptimeRecord struct {
	StartTime int64 `json:"start_time"`
}

// getXcoshDir resolves and returns the path to the ~/.xcosh directory safely.
func getXcoshDir() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "xcosh_data" // Fallback to local directory if home is inaccessible
	}
	return filepath.Join(homeDir, ".xcosh")
}

// getUptimeFilePath returns the full absolute path for the uptime tracking JSON file.
func getUptimeFilePath() string {
	dir := getXcoshDir()
	os.MkdirAll(dir, 0755)
	return filepath.Join(dir, "uptime.json")
}

var UptimeFile = getUptimeFilePath()

// RecordStartTime is invoked during node boot initialization to record the startup timestamp.
func RecordStartTime() {
	// Ensure that the target directory structure exists prior to file persistence operations.
	os.MkdirAll(getXcoshDir(), 0755)
	
	// Re-evaluate UptimeFile path in case it changed or initialized dynamically.
	UptimeFile = getUptimeFilePath()

	// If the uptime file already exists (meaning the node may already be running), do not overwrite unless newly restarted.
	if _, err := os.Stat(UptimeFile); os.IsNotExist(err) {
		record := UptimeRecord{
			StartTime: time.Now().Unix(),
		}
		data, _ := json.MarshalIndent(record, "", "  ")
		os.WriteFile(UptimeFile, data, 0644)
	}
}

// GetUptime computes how long the node has been active (in seconds, minutes, hours, days) similar to dogecoin-cli uptime.
func GetUptime() (int64, string) {
	UptimeFile = getUptimeFilePath()

	// Read the serialized uptime record from disk storage.
	data, err := os.ReadFile(UptimeFile)
	if err != nil {
		// If the file does not exist (node has never run/booted via daemon), record the current time.
		StartTime := time.Now().Unix()
		record := UptimeRecord{StartTime: StartTime}
		newData, _ := json.MarshalIndent(record, "", "  ")
		os.MkdirAll(getXcoshDir(), 0755)
		os.WriteFile(UptimeFile, newData, 0644)
		return 0, "0 seconds"
	}

	var record UptimeRecord
	// Unmarshal the retrieved uptime record JSON data into memory.
	json.Unmarshal(data, &record)

	now := time.Now().Unix()
	diff := now - record.StartTime
	if diff < 0 {
		diff = 0
	}

	// Convert total seconds into a human-readable format (Days, Hours, Minutes, Seconds).
	days := diff / 86400
	hours := (diff % 86400) / 3600
	minutes := (diff % 3600) / 60
	seconds := diff % 60

	var result string
	if days > 0 {
		result = fmt.Sprintf("%d days, %d hours, %d minutes, %d seconds", days, hours, minutes, seconds)
	} else if hours > 0 {
		result = fmt.Sprintf("%d hours, %d minutes, %d seconds", hours, minutes, seconds)
	} else if minutes > 0 {
		result = fmt.Sprintf("%d minutes, %d seconds", minutes, seconds)
	} else {
		result = fmt.Sprintf("%d seconds", seconds)
	}

	// Return both the raw duration in seconds and the formatted human-readable uptime string.
	return diff, result
}
