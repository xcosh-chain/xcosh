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

package storage

import (
	"encoding/json"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
)

// Database represents a persistent LevelDB storage engine wrapper instance.
type Database struct {
	DB *leveldb.DB
}

// NewDatabase initializes and opens a LevelDB instance at the specified directory path.
func NewDatabase(dbPath string) (*Database, error) {
	// Open the underlying LevelDB database instance at the designated file path with default options.
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	// Wrap and return the active database connection instance encapsulated within the Database structure.
	return &Database{DB: db}, nil
}

// SaveBlock serializes a block payload and commits it to disk storage, updating the latest block index pointer.
func (d *Database) SaveBlock(index uint64, blockData interface{}) error {
	// Serialize the incoming block structure instance into a JSON byte representation.
	data, err := json.Marshal(blockData)
	if err != nil {
		return err
	}

	// Construct the unique storage database key identifier corresponding to the target block height index.
	key := "block_" + strconv.FormatUint(index, 10)

	// Commit the serialized block byte payload directly into the LevelDB storage instance.
	err = d.DB.Put([]byte(key), data, nil)
	if err != nil {
		return err
	}

	// Update and persist the latest block height pointer reference key within the database.
	return d.DB.Put([]byte("last_index"), []byte(strconv.FormatUint(index, 10)), nil)
}

// GetLastIndex retrieves the highest committed block height index from the underlying database.
func (d *Database) GetLastIndex() (uint64, bool) {
	// Query the leveldb database for the reserved last index tracking key.
	data, err := d.DB.Get([]byte("last_index"), nil)
	if err != nil {
		return 0, false
	}

	// Parse the retrieved byte array representation back into an unsigned 64-bit integer index value.
	idx, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, false
	}

	// Return the parsed numerical block height index along with a success boolean flag.
	return idx, true
}

// GetBlock queries and returns the serialized byte payload of a specific block by its height index.
func (d *Database) GetBlock(index uint64) ([]byte, error) {
	// Construct the exact database key string matching the requested numerical block index.
	key := "block_" + strconv.FormatUint(index, 10)

	// Retrieve and return the raw stored block bytes from the LevelDB database instance.
	return d.DB.Get([]byte(key), nil)
}

// DeleteBlock removes a specific block entry from disk storage and decrements the last index pointer.
func (d *Database) DeleteBlock(index uint64) error {
	key := "block_" + strconv.FormatUint(index, 10)
	err := d.DB.Delete([]byte(key), nil)
	if err != nil {
		return err
	}

	// Update last_index pointer to the previous block height if applicable
	if index > 0 {
		newLast := index - 1
		return d.DB.Put([]byte("last_index"), []byte(strconv.FormatUint(newLast, 10)), nil)
	}

	return d.DB.Delete([]byte("last_index"), nil)
}

// ClearAll iterates through all keys stored in the database, batches them, and deletes them completely for hard fork resets.
func (d *Database) ClearAll() error {
	// Create an iterator to scan all keys present in the LevelDB instance.
	iter := d.DB.NewIterator(nil, nil)
	defer iter.Release()

	// Initialize a LevelDB write batch container for atomic deletion operations.
	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Execute the batch write operation to wipe out all existing keys and values from disk.
	return d.DB.Write(batch, nil)
}
