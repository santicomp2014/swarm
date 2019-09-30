// Copyright 2016 The go-ethereum Authors
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

package network

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethersphere/swarm/p2p/protocols"
	p2ptest "github.com/ethersphere/swarm/p2p/testing"
	"github.com/ethersphere/swarm/pot"
	"github.com/ethersphere/swarm/state"
)

func newHiveTester(params *HiveParams, n int, store state.Store) (*bzzTester, *Hive, error) {
	// setup
	prvkey, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}
	addr := PrivateKeyToBzzKey(prvkey)
	to := NewKademlia(addr, NewKadParams())
	pp := NewHive(params, to, store) // hive

	bt, err := newBzzBaseTester(n, prvkey, DiscoverySpec, pp.Run)
	if err != nil {
		return nil, nil, err
	}
	return bt, pp, nil
}

// TestRegisterAndConnect verifies that the protocol runs successfully
// and that the peer connection exists afterwards
func TestRegisterAndConnect(t *testing.T) {
	params := NewHiveParams()
	s, pp, err := newHiveTester(params, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	node := s.Nodes[0]
	raddr := NewBzzAddrFromEnode(node)
	pp.Register(raddr)

	// start the hive
	err = pp.Start(s.Server)
	if err != nil {
		t.Fatal(err)
	}
	defer pp.Stop()

	// both hive connect and disconect check have time delays
	// therefore we need to verify that peer is connected
	// so that we are sure that the disconnect timeout doesn't complete
	// before the hive connect method is run at least once
	timeout := time.After(time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("expected connection")
		default:
		}
		i := 0
		pp.Kademlia.EachConn(nil, 256, func(addr *Peer, po int) bool {
			i++
			return true
		})
		if i > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// check that the connection actually exists
	// the timeout error means no disconnection events
	// were received within the a certain timeout
	err = s.TestDisconnected(&p2ptest.Disconnect{
		Peer:  s.Nodes[0].ID(),
		Error: nil,
	})

	if err == nil || err.Error() != "timed out waiting for peers to disconnect" {
		t.Fatalf("expected no disconnection event")
	}
}

// TestHiveStatePersistence creates a protocol simulation with n peers for a node
// After protocols complete, the node is shut down and the state is stored.
// Another simulation is created, where 0 nodes are created, but where the stored state is passed
// The test succeeds if all the peers from the stored state are known after the protocols of the
// second simulation have completed
//
// Actual connectivity is not in scope for this test, as the peers loaded from state are not known to
// the simulation; the test only verifies that the peers are known to the node
func TestHiveStatePersistence(t *testing.T) {
	dir, err := ioutil.TempDir("", "hive_test_store")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const peersCount = 5

	startHive := func(t *testing.T, dir string) (h *Hive, cleanupFunc func()) {
		store, err := state.NewDBStore(dir)
		if err != nil {
			t.Fatal(err)
		}

		params := NewHiveParams()
		params.Discovery = false

		prvkey, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}

		h = NewHive(params, NewKademlia(PrivateKeyToBzzKey(prvkey), NewKadParams()), store)
		s := p2ptest.NewProtocolTester(prvkey, 0, func(p *p2p.Peer, rw p2p.MsgReadWriter) error { return nil })

		if err := h.Start(s.Server); err != nil {
			t.Fatal(err)
		}

		cleanupFunc = func() {
			err := h.Stop()
			if err != nil {
				t.Fatal(err)
			}

			s.Stop()
		}
		return h, cleanupFunc
	}

	h1, cleanup1 := startHive(t, dir)
	peers := make(map[string]bool)
	for i := 0; i < peersCount; i++ {
		raddr := RandomBzzAddr()
		h1.Register(raddr)
		peers[raddr.String()] = true
	}
	cleanup1()

	// start the hive and check that we know of all expected peers
	h2, cleanup2 := startHive(t, dir)
	cleanup2()

	i := 0
	h2.Kademlia.EachAddr(nil, 256, func(addr *BzzAddr, po int) bool {
		delete(peers, addr.String())
		i++
		return true
	})
	if i != peersCount {
		t.Fatalf("invalid number of entries: got %v, want %v", i, peersCount)
	}
	if len(peers) != 0 {
		t.Fatalf("%d peers left over: %v", len(peers), peers)
	}
}

/***
 *
 * - after connect, that outgoing subpeersmsg is sent
 *
 */
func TestSubPeersMsg(t *testing.T) {
	params := NewHiveParams()
	s, pp, err := newHiveTester(params, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	node := s.Nodes[0]
	raddr := NewBzzAddrFromEnode(node)
	pp.Register(raddr)

	// start the hive and wait for the connection
	pp.Start(s.Server)
	defer pp.Stop()

	// send subPeersMsg to the peer
	err = s.TestExchanges(p2ptest.Exchange{
		Label: "outgoing subPeersMsg",
		Expects: []p2ptest.Expect{
			{
				Code: 1,
				Msg:  &subPeersMsg{Depth: 0},
				Peer: node.ID(),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
}

const (
	maxPO         = 8 // PO of pivot and control; chosen to test enough cases but not run too long
	maxPeerPO     = 6 // pivot has no peers closer than this to the control peer
	maxPeersPerPO = 3
)

// TestInitialPeersMsg tests if peersMsg response to incoming subPeersMsg is correct
func TestInitialPeersMsg(t *testing.T) {
	for po := 0; po < maxPO; po++ {
		for depth := 0; depth < maxPO; depth++ {
			t.Run(fmt.Sprintf("PO=%d,advertised depth=%d", po, depth), func(t *testing.T) {
				testInitialPeersMsg(t, po, depth)
			})
		}
	}
}

// testInitialPeersMsg tests that the correct set of peer info is sent
// to another peer after receiving their subPeersMsg request
func testInitialPeersMsg(t *testing.T, peerPO, peerDepth int) {
	// generate random pivot address
	prvkey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	defer func(orig func([]*BzzAddr) []*BzzAddr) {
		sortPeers = orig
	}(sortPeers)
	sortPeers = testSortPeers
	pivotAddr := pot.NewAddressFromBytes(PrivateKeyToBzzKey(prvkey))
	// generate control peers address at peerPO wrt pivot
	peerAddr := pot.RandomAddressAt(pivotAddr, peerPO)
	// construct kademlia and hive
	to := NewKademlia(pivotAddr[:], NewKadParams())
	hive := NewHive(NewHiveParams(), to, nil)

	// expected addrs in peersMsg response
	var expBzzAddrs []*BzzAddr
	connect := func(a pot.Address, po int) (addrs []*BzzAddr) {
		n := rand.Intn(maxPeersPerPO)
		for i := 0; i < n; i++ {
			peer := newDiscPeer(pot.RandomAddressAt(a, po))
			hive.On(peer)
			addrs = append(addrs, peer.BzzAddr)
		}
		return addrs
	}
	register := func(a pot.Address, po int) {
		discPeer := newDiscPeer(a)
		hive.Register(discPeer.BzzAddr)
	}

	// generate connected and just registered peers
	for po := maxPeerPO; po >= 0; po-- {
		// create a fake connected peer at po from peerAddr
		ons := connect(peerAddr, po)
		// create a fake registered address at po from peerAddr
		register(peerAddr, po)
		// we collect expected peer addresses only up till peerPO
		if po < peerDepth {
			continue
		}
		expBzzAddrs = append(expBzzAddrs, ons...)
	}

	// add extra connections closer to pivot than control
	for po := peerPO + 1; po < maxPO; po++ {
		ons := connect(pivotAddr, po)
		if peerDepth <= peerPO {
			expBzzAddrs = append(expBzzAddrs, ons...)
		}
	}

	// create a special bzzBaseTester in which we can associate `enode.ID` to the `bzzAddr` we created above
	s, _, err := newBzzBaseTesterWithAddrs(prvkey, [][]byte{peerAddr[:]}, DiscoverySpec, hive.Run)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	// peerID to use in the protocol tester testExchange expect/trigger
	peerID := s.Nodes[0].ID()
	// block until control peer is found among hive peers
	found := false
	for attempts := 0; attempts < 2000; attempts++ {
		found = hive.Peer(peerID) != nil
		if found {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	if !found {
		t.Fatal("timeout waiting for peer connection to start")
	}

	// pivotDepth is the advertised depth of the pivot node we expect in the outgoing subPeersMsg
	pivotDepth := hive.Saturation()
	// the test exchange is as follows:
	// 1. pivot sends to the control peer a `subPeersMsg` advertising its depth (ignored)
	// 2. peer sends to pivot a `subPeersMsg` advertising its own depth (arbitrarily chosen)
	// 3. pivot responds with `peersMsg` with the set of expected peers
	err = s.TestExchanges(
		p2ptest.Exchange{
			Label: "outgoing subPeersMsg",
			Expects: []p2ptest.Expect{
				{
					Code: 1,
					Msg:  &subPeersMsg{Depth: uint8(pivotDepth)},
					Peer: peerID,
				},
			},
		},
		p2ptest.Exchange{
			Label: "trigger subPeersMsg and expect peersMsg",
			Triggers: []p2ptest.Trigger{
				{
					Code: 1,
					Msg:  &subPeersMsg{Depth: uint8(peerDepth)},
					Peer: peerID,
				},
			},
			Expects: []p2ptest.Expect{
				{
					Code:    0,
					Msg:     &peersMsg{Peers: testSortPeers(expBzzAddrs)},
					Peer:    peerID,
					Timeout: 100 * time.Millisecond,
				},
			},
		})

	// for values MaxPeerPO < peerPO < MaxPO the pivot has no peers to offer to the control peer
	// in this case, no peersMsg will be sent out, and we would run into a time out
	if len(expBzzAddrs) == 0 {
		if err != nil {
			if err.Error() != "exchange #1 \"trigger subPeersMsg and expect peersMsg\": timed out" {
				t.Fatalf("expected timeout, got %v", err)
			}
			return
		}
		t.Fatalf("expected timeout, got no error")
	}

	if err != nil {
		t.Fatal(err)
	}
}

func testSortPeers(peers []*BzzAddr) []*BzzAddr {
	comp := func(i, j int) bool {
		vi := binary.BigEndian.Uint64(peers[i].OAddr)
		vj := binary.BigEndian.Uint64(peers[j].OAddr)
		return vi < vj
	}
	sort.Slice(peers, comp)
	return peers
}

// as we are not creating a real node via the protocol,
// we need to create the discovery peer objects for the additional kademlia
// nodes manually
func newDiscPeer(addr pot.Address) *Peer {
	// deterministically create enode id
	// Input to the non-random input buffer is 2xaddress since it munches 256 bits
	addrSeed := append(addr.Bytes(), addr.Bytes()...)
	pKey, _ := ecdsa.GenerateKey(crypto.S256(), bytes.NewBuffer(addrSeed))
	pubKey := pKey.PublicKey
	nod := enode.NewV4(&pubKey, net.IPv4(127, 0, 0, 1), 0, 0)
	bzzAddr := NewBzzAddr(addr[:], []byte(nod.String()))
	id := nod.ID()
	p2pPeer := p2p.NewPeer(id, id.String(), nil)
	return NewPeer(&BzzPeer{
		Peer:    protocols.NewPeer(p2pPeer, &dummyMsgRW{}, DiscoverySpec),
		BzzAddr: bzzAddr,
	})
}

type dummyMsgRW struct{}

func (d *dummyMsgRW) ReadMsg() (p2p.Msg, error) {
	return p2p.Msg{}, nil
}
func (d *dummyMsgRW) WriteMsg(msg p2p.Msg) error {
	return nil
}
