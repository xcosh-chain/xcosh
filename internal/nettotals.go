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
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// UploadTarget represents the configuration schema for network data upload quota targets.
type UploadTarget struct {
	Timeframe             int64 `json:"timeframe"`
	Target                int64 `json:"target"`
	TargetReached         bool  `json:"target_reached"`
	ServeHistoricalBlocks bool  `json:"serve_historical_blocks"`
	BytesLeftInCycle      int64 `json:"bytes_left_in_cycle"`
	TimeLeftInCycle       int64 `json:"time_left_in_cycle"`
}

// NetTotalsResponse defines the output JSON format structure for the getnettotals command.
type NetTotalsResponse struct {
	TotalBytesRecv int64        `json:"totalbytesrecv"`
	TotalBytesSent int64        `json:"totalbytessent"`
	TimeMillis     int64        `json:"timemillis"`
	UploadTarget   UploadTarget `json:"uploadtarget"`
}

var (
	totalBytesRecvAtomic atomic.Int64
	totalBytesSentAtomic atomic.Int64
)

// RecordReceive increments the total incoming byte counter for network data traffic.
func RecordReceive(n int) {
	totalBytesRecvAtomic.Add(int64(n))
}

// RecordSend increments the total outgoing byte counter for network data traffic.
func RecordSend(n int) {
	totalBytesSentAtomic.Add(int64(n))
}

// GetNetTotals aggregates and returns current node network traffic statistical data.
func GetNetTotals() NetTotalsResponse {
	return NetTotalsResponse{
		TotalBytesRecv: totalBytesRecvAtomic.Load(),
		TotalBytesSent: totalBytesSentAtomic.Load(),
		TimeMillis:     time.Now().UnixMilli(),
		UploadTarget: UploadTarget{
			Timeframe:             86400,
			Target:                0,
			TargetReached:         false,
			ServeHistoricalBlocks: true,
			BytesLeftInCycle:      0,
			TimeLeftInCycle:       0,
		},
	}
}

// RegisterNetTotalsHandler registers the HTTP route handler for managing getnettotals RPC requests.
func RegisterNetTotalsHandler(mux *http.ServeMux) {
	mux.HandleFunc("/getnettotals", func(w http.ResponseWriter, r *http.Request) {
		totals := GetNetTotals()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"result": totals,
			"error":  nil,
			"id":     1,
		})
	})
}
