// Copyright 2019 The go-ethereum Authors
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

package pushsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/network"
	"github.com/ethersphere/swarm/spancontext"
	"github.com/ethersphere/swarm/storage"
	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

// DB interface implemented by localstore
type DB interface {
	// subscribe to chunk to be push synced - iterates from earliest to newest
	SubscribePush(context.Context) (<-chan storage.Chunk, func())
	// called to set a chunk as synced - and allow it to be garbage collected
	// TODO this should take ... last argument to delete many in one batch
	Set(context.Context, chunk.ModeSet, ...chunk.Address) error
}

// Pusher takes care of the push syncing
type Pusher struct {
	kad      *network.Kademlia
	store    DB                     // localstore DB
	tags     *chunk.Tags            // tags to update counts
	quit     chan struct{}          // channel to signal quitting on all loops
	pushed   map[string]*pushedItem // cache of items push-synced
	receipts chan *receiptMsg       // channel to receive receipts
	ps       PubSub                 // PubSub interface to send chunks and receive receipts
}

var (
	retryInterval = 30 * time.Second // seconds to wait before retry sync
)

// pushedItem captures the info needed for the pusher about a chunk during the
// push-sync--receipt roundtrip
type pushedItem struct {
	tag    *chunk.Tag // tag for the chunk
	sentAt time.Time  // most recently sent at time
	synced bool       // set when chunk got synced

	// roundtrip span
	sp opentracing.Span
}

// NewPusher contructs a Pusher and starts up the push sync protocol
// takes
// - a DB interface to subscribe to push sync index to allow iterating over recently stored chunks
// - a pubsub interface to send chunks and receive statements of custody
// - tags that hold the several tag
func NewPusher(store DB, ps PubSub, tags *chunk.Tags, kad *network.Kademlia) *Pusher {
	p := &Pusher{
		kad:      kad,
		store:    store,
		tags:     tags,
		quit:     make(chan struct{}),
		pushed:   make(map[string]*pushedItem),
		receipts: make(chan *receiptMsg),
		ps:       ps,
	}
	go p.sync()
	return p
}

// Close closes the pusher
func (p *Pusher) Close() {
	close(p.quit)
}

// sync starts a forever loop that pushes chunks to their neighbourhood
// and receives receipts (statements of custody) for them.
// chunks that are not acknowledged with a receipt are retried
// not earlier than retryInterval after they were last pushed
// the routine also updates counts of states on a tag in order
// to monitor the proportion of saved, sent and synced chunks of
// a file or collection
func (p *Pusher) sync() {
	var chunks <-chan chunk.Chunk
	var cancel, stop func()
	var ctx context.Context
	var synced []storage.Address

	// timer
	timer := time.NewTimer(0)
	defer timer.Stop()

	// register handler for pssReceiptTopic on pss pubsub
	deregister := p.ps.Register(pssReceiptTopic, false, func(msg []byte, _ *p2p.Peer) error {
		return p.handleReceiptMsg(msg)
	})
	defer deregister()

	chunksInBatch := -1
	var batchStartTime time.Time

	for {
		select {

		// retry interval timer triggers starting from new
		case <-timer.C:
			metrics.GetOrRegisterCounter("pusher.subscribe-push", nil).Inc(1)
			// TODO: implement some smart retry strategy relying on sent/synced ratio change
			// if subscribe was running, stop it
			if stop != nil {
				stop()
			}
			for _, addr := range synced {
				// set chunk status to synced, insert to db GC index
				if err := p.store.Set(context.Background(), chunk.ModeSetSync, addr); err != nil {
					log.Warn("error setting chunk to synced", "addr", addr, "err", err)
					continue
				}
				delete(p.pushed, addr.Hex())
			}
			// reset synced list
			synced = nil

			// we don't want to record the first iteration
			if chunksInBatch != -1 {
				// this measurement is not a timer, but we want a histogram, so it fits the data structure
				metrics.GetOrRegisterResettingTimer("pusher.subscribe-push.chunks-in-batch.hist", nil).Update(time.Duration(chunksInBatch))

				metrics.GetOrRegisterResettingTimer("pusher.subscribe-push.chunks-in-batch.time", nil).UpdateSince(batchStartTime)
				metrics.GetOrRegisterCounter("pusher.subscribe-push.chunks-in-batch", nil).Inc(int64(chunksInBatch))
			}
			chunksInBatch = 0
			batchStartTime = time.Now()

			// and start iterating on Push index from the beginning
			ctx, cancel = context.WithCancel(context.Background())
			chunks, stop = p.store.SubscribePush(ctx)
			// reset timer to go off after retryInterval
			timer.Reset(retryInterval)

		// handle incoming chunks
		case ch, more := <-chunks:
			func() {
				chunksInBatch++
				// if no more, set to nil and wait for timer
				if !more {
					chunks = nil
					return
				}

				metrics.GetOrRegisterCounter("pusher.send-chunk", nil).Inc(1)
				// if no need to sync this chunk then continue
				if !p.needToSync(ch) {
					return
				}

				metrics.GetOrRegisterCounter("pusher.send-chunk.send-to-sync", nil).Inc(1)
				// send the chunk and ignore the error
				go func(ch chunk.Chunk) {
					if err := p.sendChunkMsg(ch); err != nil {
						log.Error("error sending chunk", "addr", ch.Address(), "err", err)
					}
				}(ch)
			}()

		// handle incoming receipts
		case rec := <-p.receipts:
			addr := chunk.Address(rec.Addr)
			origin := rec.Origin
			func() {
				metrics.GetOrRegisterCounter("pusher.receipts.all", nil).Inc(1)
				log.Debug("synced", "addr", addr)
				// ignore if already received receipt
				item, found := p.pushed[addr.Hex()]
				if !found {
					metrics.GetOrRegisterCounter("pusher.receipts.not-found", nil).Inc(1)
					log.Debug("not wanted or already got... ignore", "addr", addr)
					return
				}
				if item.synced {
					metrics.GetOrRegisterCounter("pusher.receipts.already-synced", nil).Inc(1)
					log.Debug("just synced... ignore", "addr", addr)
					return
				}

				timediff := time.Now().Sub(item.sentAt)
				log.Debug("time to sync", "dur", timediff)

				metrics.GetOrRegisterResettingTimer("pusher.chunk.roundtrip", nil).Update(timediff)

				metrics.GetOrRegisterCounter("pusher.receipts.synced", nil).Inc(1)
				// collect synced addresses
				synced = append(synced, addr)
				// set synced flag
				item.synced = true
				// increment synced count for the tag if exists
				if item.tag != nil {
					item.tag.Inc(chunk.StateSynced)

					item.sp.LogFields(olog.String("ro", fmt.Sprintf("%x", origin)))
					item.sp.Finish()

					if item.tag.DoneSyncing() {
						log.Info("closing root span for tag", "taguid", item.tag.Uid, "tagname", item.tag.Name)
						item.tag.FinishRootSpan()
					}
				}
			}()

		case <-p.quit:
			// if there was a subscription, cancel it
			if cancel != nil {
				cancel()
			}
			return
		}
	}
}

// handleReceiptMsg is a handler for pssReceiptTopic that
// - deserialises receiptMsg and
// - sends the receipted address on a channel
func (p *Pusher) handleReceiptMsg(msg []byte) error {
	receipt, err := decodeReceiptMsg(msg)
	if err != nil {
		return err
	}
	log.Debug("Handler", "receipt", label(receipt.Addr), "self", label(p.ps.BaseAddr()))
	go p.PushReceipt(receipt.Addr, receipt.Origin)
	return nil
}

// pushReceipt just inserts the address into the channel
// it is also called by the push sync Storer if the originator and storer identical
func (p *Pusher) PushReceipt(addr []byte, origin []byte) {
	r := &receiptMsg{
		addr,
		origin,
		[]byte{},
	}
	select {
	case p.receipts <- r:
	case <-p.quit:
	}
}

// sendChunkMsg sends chunks to their destination
// using the PubSub interface Send method (e.g., pss neighbourhood addressing)
func (p *Pusher) sendChunkMsg(ch chunk.Chunk) error {
	rlpTimer := time.Now()

	cmsg := &chunkMsg{
		Origin: p.ps.BaseAddr(),
		Addr:   ch.Address()[:],
		Data:   ch.Data(),
		Nonce:  newNonce(),
	}
	msg, err := rlp.EncodeToBytes(cmsg)
	if err != nil {
		return err
	}
	log.Debug("send chunk", "addr", label(ch.Address()), "self", label(p.ps.BaseAddr()))

	metrics.GetOrRegisterResettingTimer("pusher.send.chunk.rlp", nil).UpdateSince(rlpTimer)

	defer metrics.GetOrRegisterResettingTimer("pusher.send.chunk.pss", nil).UpdateSince(time.Now())
	return p.ps.Send(ch.Address()[:], pssChunkTopic, msg)
}

// needToSync checks if a chunk needs to be push-synced:
// * if not sent yet OR
// * if sent but more then retryInterval ago, so need resend
func (p *Pusher) needToSync(ch chunk.Chunk) bool {
	item, found := p.pushed[ch.Address().Hex()]
	// has been pushed already
	if found {
		// has synced already since subscribe called
		if item.synced {
			return false
		}
		// too early to retry
		if item.sentAt.Add(retryInterval).After(time.Now()) {
			return false
		}
		// first time encountered
	} else {

		addr := ch.Address()

		// remember item
		tag, _ := p.tags.Get(ch.TagID())
		item = &pushedItem{
			tag: tag,
		}

		// should i sync??
		if !p.kad.CloserPeerThanMe(addr) {
			// no =>

			// mark as synced
			// set chunk status to synced, insert to db GC index
			if err := p.store.Set(context.Background(), chunk.ModeSetSync, addr); err != nil {
				log.Warn("error setting chunk to synced", "addr", addr, "err", err)
			}

			// set synced flag
			item.synced = true
			// increment synced count for the tag if exists
			if item.tag != nil {
				item.tag.Inc(chunk.StateSynced)

				// opentracing for self chunks that don't need syncing?
				_, osp := spancontext.StartSpan(
					tag.Tctx,
					"chunk.mine")
				osp.LogFields(olog.String("ref", ch.Address().String()))
				osp.SetTag("addr", ch.Address().String())
				osp.Finish()

				if item.tag.DoneSyncing() {
					log.Info("closing root span for tag", "taguid", item.tag.Uid, "tagname", item.tag.Name)
					item.tag.FinishRootSpan()
				}
			}

			return false
		}
		// i should sync

		// increment SENT count on tag  if it exists
		if tag != nil {
			tag.Inc(chunk.StateSent)

			// opentracing for chunk roundtrip
			_, osp := spancontext.StartSpan(
				tag.Tctx,
				"chunk.sent")
			osp.LogFields(olog.String("ref", ch.Address().String()))
			osp.SetTag("addr", ch.Address().String())

			item.sp = osp
		}

		// remember the item
		p.pushed[ch.Address().Hex()] = item
	}
	item.sentAt = time.Now()
	return true
}
