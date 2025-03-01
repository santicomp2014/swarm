// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package localstore

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// Set updates database indexes for
// chunks represented by provided addresses.
// Set is required to implement chunk.Store
// interface.
func (db *DB) Set(ctx context.Context, mode chunk.ModeSet, addrs ...chunk.Address) (err error) {
	metricName := fmt.Sprintf("localstore.Set.%s", mode)

	metrics.GetOrRegisterCounter(metricName, nil).Inc(1)
	defer totalTimeMetric(metricName, time.Now())
	err = db.set(mode, addrs...)
	if err != nil {
		metrics.GetOrRegisterCounter(metricName+".error", nil).Inc(1)
	}
	return err
}

// set updates database indexes for
// chunks represented by provided addresses.
// It acquires lockAddr to protect two calls
// of this function for the same address in parallel.
func (db *DB) set(mode chunk.ModeSet, addrs ...chunk.Address) (err error) {
	// protect parallel updates
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	batch := new(leveldb.Batch)

	// variables that provide information for operations
	// to be done after write batch function successfully executes
	var gcSizeChange int64                      // number to add or subtract from gcSize
	triggerPullFeed := make(map[uint8]struct{}) // signal pull feed subscriptions to iterate

	switch mode {
	case chunk.ModeSetAccess:
		// A lazy populated map of bin ids to properly set
		// BinID values for new chunks based on initial value from database
		// and incrementing them.
		binIDs := make(map[uint8]uint64)
		for _, addr := range addrs {
			po := db.po(addr)
			c, err := db.setAccess(batch, binIDs, addr, po)
			if err != nil {
				return err
			}
			gcSizeChange += c
			triggerPullFeed[po] = struct{}{}
		}
		for po, id := range binIDs {
			db.binIDs.PutInBatch(batch, uint64(po), id)
		}

	case chunk.ModeSetSyncPush, chunk.ModeSetSyncPull:
		for _, addr := range addrs {
			c, err := db.setSync(batch, addr, mode)
			if err != nil {
				return err
			}
			gcSizeChange += c
		}

	case chunk.ModeSetRemove:
		for _, addr := range addrs {
			c, err := db.setRemove(batch, addr)
			if err != nil {
				return err
			}
			gcSizeChange += c
		}

	case chunk.ModeSetPin:
		for _, addr := range addrs {
			err := db.setPin(batch, addr)
			if err != nil {
				return err
			}
		}
	case chunk.ModeSetUnpin:
		for _, addr := range addrs {
			err := db.setUnpin(batch, addr)
			if err != nil {
				return err
			}
		}

	default:
		return ErrInvalidMode
	}

	err = db.incGCSizeInBatch(batch, gcSizeChange)
	if err != nil {
		return err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}
	for po := range triggerPullFeed {
		db.triggerPullSubscriptions(po)
	}
	return nil
}

// setAccess sets the chunk access time by updating required indexes:
//  - add to pull, insert to gc
// Provided batch and binID map are updated.
func (db *DB) setAccess(batch *leveldb.Batch, binIDs map[uint8]uint64, addr chunk.Address, po uint8) (gcSizeChange int64, err error) {

	item := addressToItem(addr)

	// need to get access timestamp here as it is not
	// provided by the access function, and it is not
	// a property of a chunk provided to Accessor.Put.
	i, err := db.retrievalDataIndex.Get(item)
	switch err {
	case nil:
		item.StoreTimestamp = i.StoreTimestamp
		item.BinID = i.BinID
	case leveldb.ErrNotFound:
		db.pushIndex.DeleteInBatch(batch, item)
		item.StoreTimestamp = now()
		item.BinID, err = db.incBinID(binIDs, po)
		if err != nil {
			return 0, err
		}
	default:
		return 0, err
	}

	i, err = db.retrievalAccessIndex.Get(item)
	switch err {
	case nil:
		item.AccessTimestamp = i.AccessTimestamp
		db.gcIndex.DeleteInBatch(batch, item)
		gcSizeChange--
	case leveldb.ErrNotFound:
		// the chunk is not accessed before
	default:
		return 0, err
	}
	item.AccessTimestamp = now()
	db.retrievalAccessIndex.PutInBatch(batch, item)
	db.pullIndex.PutInBatch(batch, item)
	db.gcIndex.PutInBatch(batch, item)
	gcSizeChange++

	return gcSizeChange, nil
}

// setSync adds the chunk to the garbage collection after syncing by updating indexes:
//  - delete from push, insert to gc
// Provided batch is updated.
func (db *DB) setSync(batch *leveldb.Batch, addr chunk.Address, mode chunk.ModeSet) (gcSizeChange int64, err error) {
	item := addressToItem(addr)

	// need to get access timestamp here as it is not
	// provided by the access function, and it is not
	// a property of a chunk provided to Accessor.Put.

	i, err := db.retrievalDataIndex.Get(item)
	if err != nil {
		if err == leveldb.ErrNotFound {
			// chunk is not found,
			// no need to update gc index
			// just delete from the push index
			// if it is there
			db.pushIndex.DeleteInBatch(batch, item)
			return 0, nil
		}
		return 0, err
	}
	item.StoreTimestamp = i.StoreTimestamp
	item.BinID = i.BinID

	// moveToGc toggles the deletion of the item from pushsync index
	// it will be false in the case pull sync was called but push sync was meant on that chunk (!tag.Anonymous)
	moveToGc := true
	if db.tags != nil {
		i, err = db.pushIndex.Get(item)
		switch err {
		case nil:
			tag, _ := db.tags.Get(i.Tag)
			if tag != nil {
				if tag.Anonymous {
					// anonymous - only pull sync
					switch mode {
					case chunk.ModeSetSyncPull:
						// this will not get called twice because we remove the item once after the !moveToGc check
						tag.Inc(chunk.StateSent)
						// this is needed since pushsync is checking if `tag.Done(chunk.StateSynced)` and when overlapping
						// chunks are synced by both push and pull sync we have a problem. as an interim solution we increment this too
						tag.Inc(chunk.StateSynced)
					case chunk.ModeSetSyncPush:
						// do nothing - this should not be possible. just log and return
						log.Warn("chunk marked as push-synced but should be anonymous!", "tag.Uid", tag.Uid)
						moveToGc = false
					}
				} else {
					// not anonymous - push and pull should be used
					switch mode {
					case chunk.ModeSetSyncPull:
						moveToGc = false
					case chunk.ModeSetSyncPush:
						tag.Inc(chunk.StateSynced)
					}
				}
			}

		case leveldb.ErrNotFound:
			// the chunk is not accessed before
		default:
			return 0, err
		}
	}
	if !moveToGc {
		return 0, nil
	}

	i, err = db.retrievalAccessIndex.Get(item)
	switch err {
	case nil:
		item.AccessTimestamp = i.AccessTimestamp
		db.gcIndex.DeleteInBatch(batch, item)
		gcSizeChange--
	case leveldb.ErrNotFound:
		// the chunk is not accessed before
	default:
		return 0, err
	}
	item.AccessTimestamp = now()
	db.retrievalAccessIndex.PutInBatch(batch, item)
	db.pushIndex.DeleteInBatch(batch, item)

	// Add in gcIndex only if this chunk is not pinned
	ok, err := db.pinIndex.Has(item)
	if err != nil {
		return 0, err
	}
	if !ok {
		db.gcIndex.PutInBatch(batch, item)
		gcSizeChange++
	}

	return gcSizeChange, nil
}

// setRemove removes the chunk by updating indexes:
//  - delete from retrieve, pull, gc
// Provided batch is updated.
func (db *DB) setRemove(batch *leveldb.Batch, addr chunk.Address) (gcSizeChange int64, err error) {
	item := addressToItem(addr)

	// need to get access timestamp here as it is not
	// provided by the access function, and it is not
	// a property of a chunk provided to Accessor.Put.
	i, err := db.retrievalAccessIndex.Get(item)
	switch err {
	case nil:
		item.AccessTimestamp = i.AccessTimestamp
	case leveldb.ErrNotFound:
	default:
		return 0, err
	}
	i, err = db.retrievalDataIndex.Get(item)
	if err != nil {
		return 0, err
	}
	item.StoreTimestamp = i.StoreTimestamp
	item.BinID = i.BinID

	db.retrievalDataIndex.DeleteInBatch(batch, item)
	db.retrievalAccessIndex.DeleteInBatch(batch, item)
	db.pullIndex.DeleteInBatch(batch, item)
	db.gcIndex.DeleteInBatch(batch, item)
	// a check is needed for decrementing gcSize
	// as delete is not reporting if the key/value pair
	// is deleted or not
	if _, err := db.gcIndex.Get(item); err == nil {
		gcSizeChange = -1
	}

	return gcSizeChange, nil
}

// setPin increments pin counter for the chunk by updating
// pin index and sets the chunk to be excluded from garbage collection.
// Provided batch is updated.
func (db *DB) setPin(batch *leveldb.Batch, addr chunk.Address) (err error) {
	item := addressToItem(addr)

	// Get the existing pin counter of the chunk
	existingPinCounter := uint64(0)
	pinnedChunk, err := db.pinIndex.Get(item)
	if err != nil {
		if err == leveldb.ErrNotFound {
			// If this Address is not present in DB, then its a new entry
			existingPinCounter = 0

			// Add in gcExcludeIndex of the chunk is not pinned already
			db.gcExcludeIndex.PutInBatch(batch, item)
		} else {
			return err
		}
	} else {
		existingPinCounter = pinnedChunk.PinCounter
	}

	// Otherwise increase the existing counter by 1
	item.PinCounter = existingPinCounter + 1
	db.pinIndex.PutInBatch(batch, item)

	return nil
}

// setUnpin decrements pin counter for the chunk by updating pin index.
// Provided batch is updated.
func (db *DB) setUnpin(batch *leveldb.Batch, addr chunk.Address) (err error) {
	item := addressToItem(addr)

	// Get the existing pin counter of the chunk
	pinnedChunk, err := db.pinIndex.Get(item)
	if err != nil {
		return err
	}

	// Decrement the pin counter or
	// delete it from pin index if the pin counter has reached 0
	if pinnedChunk.PinCounter > 1 {
		item.PinCounter = pinnedChunk.PinCounter - 1
		db.pinIndex.PutInBatch(batch, item)
	} else {
		db.pinIndex.DeleteInBatch(batch, item)
	}

	return nil
}
