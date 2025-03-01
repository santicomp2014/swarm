// Copyright 2017 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/pkg/reexec"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm"
	"github.com/ethersphere/swarm/api"
	"github.com/ethersphere/swarm/swap"
	"github.com/ethersphere/swarm/testutil"
)

func TestConfigDump(t *testing.T) {
	swarm := runSwarm(t, "--verbosity", fmt.Sprintf("%d", *testutil.Loglevel), "dumpconfig")
	defaultConf := api.NewConfig()
	out, err := tomlSettings.Marshal(&defaultConf)
	if err != nil {
		t.Fatal(err)
	}
	swarm.Expect(string(out))
	swarm.ExpectExit()
}

func TestBzzKeyFlag(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal("unable to generate key")
	}
	hexKey := hex.EncodeToString(crypto.FromECDSA(key))
	bzzaccount := crypto.PubkeyToAddress(key.PublicKey).Hex()

	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := &node.Config{
		DataDir: dir,
		IPCPath: "testbzzkeyflag.ipc",
		NoUSB:   true,
	}

	node := &testNode{Dir: dir}

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "42",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.ListenPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		fmt.Sprintf("--%s", utils.IPCPathFlag.Name), conf.IPCPath,
		fmt.Sprintf("--%s", SwarmBzzKeyHexFlag.Name), hexKey,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}

	node.Cmd = runSwarm(t, flags...)
	defer func() {
		node.Shutdown()
	}()

	node.Cmd.InputLine(testPassphrase)

	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}
	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.BzzAccount != bzzaccount {
		t.Fatalf("Expected account to be %s, got %s", bzzaccount, info.BzzAccount)
	}
}
func TestEmptyBzzAccountFlagMultipleAccounts(t *testing.T) {
	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create two accounts on disk
	getTestAccount(t, dir)
	getTestAccount(t, dir)

	node := &testNode{Dir: dir}

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "42",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.ListenPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}

	node.Cmd = runSwarm(t, flags...)

	node.Cmd.ExpectRegexp(fmt.Sprintf("Please choose one of the accounts by running swarm with the --%s flag.", SwarmAccountFlag.Name))
}

func TestEmptyBzzAccountFlagSingleAccount(t *testing.T) {
	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf, account := getTestAccount(t, dir)
	node := &testNode{Dir: dir}

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "42",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.ListenPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		fmt.Sprintf("--%s", utils.IPCPathFlag.Name), conf.IPCPath,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}

	node.Cmd = runSwarm(t, flags...)
	defer func() {
		node.Shutdown()
	}()

	node.Cmd.InputLine(testPassphrase)

	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}
	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.BzzAccount != account.Address.Hex() {
		t.Fatalf("Expected account to be %s, got %s", account.Address.Hex(), info.BzzAccount)
	}
}

func TestEmptyBzzAccountFlagNoAccountWrongPassword(t *testing.T) {
	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	node := &testNode{Dir: dir}

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "42",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.ListenPortFlag.Name), "0",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}

	node.Cmd = runSwarm(t, flags...)

	// Set password
	node.Cmd.InputLine("goodpassword")
	// Confirm password
	node.Cmd.InputLine("wrongpassword")

	node.Cmd.ExpectRegexp("Passphrases do not match")
}

func TestConfigCmdLineOverrides(t *testing.T) {
	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf, account := getTestAccount(t, dir)
	node := &testNode{Dir: dir}

	// assign ports
	httpPort, err := assignTCPPort()
	if err != nil {
		t.Fatal(err)
	}

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "42",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), httpPort,
		fmt.Sprintf("--%s", utils.ListenPortFlag.Name), "0",
		fmt.Sprintf("--%s", SwarmSyncModeFlag.Name), "none",
		fmt.Sprintf("--%s", CorsStringFlag.Name), "*",
		fmt.Sprintf("--%s", SwarmAccountFlag.Name), account.Address.String(),
		fmt.Sprintf("--%s", SwarmDeliverySkipCheckFlag.Name),
		fmt.Sprintf("--%s", EnsAPIFlag.Name), "",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		fmt.Sprintf("--%s", utils.IPCPathFlag.Name), conf.IPCPath,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
		fmt.Sprintf("--%s", SwarmSwapPaymentThresholdFlag.Name), strconv.FormatUint(swap.DefaultPaymentThreshold+1, 10),
		fmt.Sprintf("--%s", SwarmSwapDisconnectThresholdFlag.Name), strconv.FormatUint(swap.DefaultDisconnectThreshold+1, 10),
		fmt.Sprintf("--%s", SwarmEnablePinningFlag.Name),
	}

	node.Cmd = runSwarm(t, flags...)
	node.Cmd.InputLine(testPassphrase)
	defer func() {
		if t.Failed() {
			node.Shutdown()
		}
	}()
	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}
	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.Port != httpPort {
		t.Fatalf("Expected port to be %s, got %s", httpPort, info.Port)
	}

	if info.NetworkID != 42 {
		t.Fatalf("Expected network ID to be %d, got %d", 42, info.NetworkID)
	}

	if info.SyncEnabled {
		t.Fatal("Expected Sync to be disabled, but is true")
	}

	if info.PushSyncEnabled {
		t.Fatal("Expected Push Sync to be disabled, but is true")
	}

	if !info.DeliverySkipCheck {
		t.Fatal("Expected DeliverySkipCheck to be enabled, but it is not")
	}

	if info.Cors != "*" {
		t.Fatalf("Expected Cors flag to be set to %s, got %s", "*", info.Cors)
	}

	if info.SwapPaymentThreshold != (swap.DefaultPaymentThreshold + 1) {
		t.Fatalf("Expected SwapPaymentThreshold to be %d, but got %d", swap.DefaultPaymentThreshold+1, info.SwapPaymentThreshold)
	}

	if info.SwapDisconnectThreshold != (swap.DefaultDisconnectThreshold + 1) {
		t.Fatalf("Expected SwapDisconnectThreshold to be %d, but got %d", swap.DefaultDisconnectThreshold+1, info.SwapDisconnectThreshold)
	}

	if info.EnablePinning != true {
		t.Fatalf("expected EnablePinning to be %t but got %t", true, info.EnablePinning)
	}

	node.Shutdown()
}

func TestConfigFileOverrides(t *testing.T) {

	// assign ports
	httpPort, err := assignTCPPort()
	if err != nil {
		t.Fatal(err)
	}

	//create a config file
	//first, create a default conf
	defaultConf := api.NewConfig()
	//change some values in order to test if they have been loaded
	defaultConf.SyncEnabled = false
	defaultConf.DeliverySkipCheck = true
	defaultConf.NetworkID = 54
	defaultConf.Port = httpPort
	defaultConf.DbCapacity = 9000000
	defaultConf.HiveParams.KeepAliveInterval = 6000000000
	//defaultConf.SyncParams.KeyBufferSize = 512
	//create a TOML string
	out, err := tomlSettings.Marshal(&defaultConf)
	if err != nil {
		t.Fatalf("Error creating TOML file in TestFileOverride: %v", err)
	}
	//create file
	f, err := ioutil.TempFile("", "testconfig.toml")
	if err != nil {
		t.Fatalf("Error writing TOML file in TestFileOverride: %v", err)
	}
	//write file
	_, err = f.WriteString(string(out))
	if err != nil {
		t.Fatalf("Error writing TOML file in TestFileOverride: %v", err)
	}
	f.Sync()

	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	conf, account := getTestAccount(t, dir)
	node := &testNode{Dir: dir}

	flags := []string{
		fmt.Sprintf("--%s", SwarmTomlConfigPathFlag.Name), f.Name(),
		fmt.Sprintf("--%s", SwarmAccountFlag.Name), account.Address.String(),
		fmt.Sprintf("--%s", EnsAPIFlag.Name), "",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		fmt.Sprintf("--%s", utils.IPCPathFlag.Name), conf.IPCPath,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}
	node.Cmd = runSwarm(t, flags...)
	node.Cmd.InputLine(testPassphrase)
	defer func() {
		if t.Failed() {
			node.Shutdown()
		}
	}()
	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}
	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.Port != httpPort {
		t.Fatalf("Expected port to be %s, got %s", httpPort, info.Port)
	}

	if info.NetworkID != 54 {
		t.Fatalf("Expected network ID to be %d, got %d", 54, info.NetworkID)
	}

	if info.SyncEnabled {
		t.Fatal("Expected Sync to be disabled, but is true")
	}

	if !info.DeliverySkipCheck {
		t.Fatal("Expected DeliverySkipCheck to be enabled, but it is not")
	}

	if info.DbCapacity != 9000000 {
		t.Fatalf("Expected DbCapacity to be %d, got %d", 9000000, info.DbCapacity)
	}

	if info.HiveParams.KeepAliveInterval != 6000000000 {
		t.Fatalf("Expected HiveParams KeepAliveInterval to be %d, got %d", uint64(6000000000), uint64(info.HiveParams.KeepAliveInterval))
	}

	node.Shutdown()
}

func TestConfigEnvVars(t *testing.T) {
	// assign ports
	httpPort, err := assignTCPPort()
	if err != nil {
		t.Fatal(err)
	}

	envVars := os.Environ()
	envVars = append(envVars, fmt.Sprintf("%s=%s", SwarmPortFlag.EnvVar, httpPort))
	envVars = append(envVars, fmt.Sprintf("%s=%s", SwarmNetworkIdFlag.EnvVar, "999"))
	envVars = append(envVars, fmt.Sprintf("%s=%s", CorsStringFlag.EnvVar, "*"))
	envVars = append(envVars, fmt.Sprintf("%s=%s", SwarmSyncModeFlag.EnvVar, "none"))
	envVars = append(envVars, fmt.Sprintf("%s=%s", SwarmDeliverySkipCheckFlag.EnvVar, "true"))

	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	conf, account := getTestAccount(t, dir)
	node := &testNode{Dir: dir}
	flags := []string{
		fmt.Sprintf("--%s", SwarmAccountFlag.Name), account.Address.String(),
		"--ens-api", "",
		"--datadir", dir,
		"--ipcpath", conf.IPCPath,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}

	//node.Cmd = runSwarm(t,flags...)
	//node.Cmd.cmd.Env = envVars
	//the above assignment does not work, so we need a custom Cmd here in order to pass envVars:
	cmd := &exec.Cmd{
		Path:   reexec.Self(),
		Args:   append([]string{"swarm-test"}, flags...),
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
	cmd.Env = envVars
	//stdout, err := cmd.StdoutPipe()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//stdout = bufio.NewReader(stdout)
	var stdin io.WriteCloser
	if stdin, err = cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	//cmd.InputLine(testPassphrase)
	io.WriteString(stdin, testPassphrase+"\n")
	defer func() {
		if t.Failed() {
			node.Shutdown()
			cmd.Process.Kill()
		}
	}()
	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}

	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.Port != httpPort {
		t.Fatalf("Expected port to be %s, got %s", httpPort, info.Port)
	}

	if info.NetworkID != 999 {
		t.Fatalf("Expected network ID to be %d, got %d", 999, info.NetworkID)
	}

	if info.Cors != "*" {
		t.Fatalf("Expected Cors flag to be set to %s, got %s", "*", info.Cors)
	}

	if info.SyncEnabled {
		t.Fatal("Expected Sync to be disabled, but is true")
	}

	if info.PushSyncEnabled {
		t.Fatal("Expected Push Sync to be disabled, but is true")
	}

	if !info.DeliverySkipCheck {
		t.Fatal("Expected DeliverySkipCheck to be enabled, but it is not")
	}

	node.Shutdown()
	cmd.Process.Kill()
}

func TestConfigCmdLineOverridesFile(t *testing.T) {

	// assign ports
	httpPort, err := assignTCPPort()
	if err != nil {
		t.Fatal(err)
	}

	//create a config file
	//first, create a default conf
	defaultConf := api.NewConfig()
	//change some values in order to test if they have been loaded
	defaultConf.SyncEnabled = true
	defaultConf.PushSyncEnabled = true
	defaultConf.NetworkID = 54
	defaultConf.Port = "8588"
	defaultConf.DbCapacity = 9000000
	defaultConf.HiveParams.KeepAliveInterval = 6000000000
	//defaultConf.SyncParams.KeyBufferSize = 512
	//create a TOML file
	out, err := tomlSettings.Marshal(&defaultConf)
	if err != nil {
		t.Fatalf("Error creating TOML file in TestFileOverride: %v", err)
	}
	//write file
	fname := "testconfig.toml"
	f, err := ioutil.TempFile("", fname)
	if err != nil {
		t.Fatalf("Error writing TOML file in TestFileOverride: %v", err)
	}
	defer os.Remove(fname)
	//write file
	_, err = f.WriteString(string(out))
	if err != nil {
		t.Fatalf("Error writing TOML file in TestFileOverride: %v", err)
	}
	f.Sync()

	dir, err := ioutil.TempDir("", "bzztest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	conf, account := getTestAccount(t, dir)
	node := &testNode{Dir: dir}

	expectNetworkId := uint64(77)

	flags := []string{
		fmt.Sprintf("--%s", SwarmNetworkIdFlag.Name), "77",
		fmt.Sprintf("--%s", SwarmPortFlag.Name), httpPort,
		fmt.Sprintf("--%s", SwarmSyncModeFlag.Name), "none",
		fmt.Sprintf("--%s", SwarmTomlConfigPathFlag.Name), f.Name(),
		fmt.Sprintf("--%s", SwarmAccountFlag.Name), account.Address.String(),
		fmt.Sprintf("--%s", EnsAPIFlag.Name), "",
		fmt.Sprintf("--%s", utils.DataDirFlag.Name), dir,
		fmt.Sprintf("--%s", utils.IPCPathFlag.Name), conf.IPCPath,
		"--verbosity", fmt.Sprintf("%d", *testutil.Loglevel),
	}
	node.Cmd = runSwarm(t, flags...)
	node.Cmd.InputLine(testPassphrase)
	defer func() {
		if t.Failed() {
			node.Shutdown()
		}
	}()
	// wait for the node to start
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(50 * time.Millisecond) {
		node.Client, err = rpc.Dial(conf.IPCEndpoint())
		if err == nil {
			break
		}
	}
	if node.Client == nil {
		t.Fatal(err)
	}

	// load info
	var info swarm.Info
	if err := node.Client.Call(&info, "bzz_info"); err != nil {
		t.Fatal(err)
	}

	if info.Port != httpPort {
		t.Fatalf("Expected port to be %s, got %s", httpPort, info.Port)
	}

	if info.NetworkID != expectNetworkId {
		t.Fatalf("Expected network ID to be %d, got %d", expectNetworkId, info.NetworkID)
	}

	if info.SyncEnabled {
		t.Fatal("Expected Sync to be disabled, but is true")
	}

	if info.PushSyncEnabled {
		t.Fatal("Expected Push Sync to be disabled, but is true")
	}

	if info.DbCapacity != 9000000 {
		t.Fatalf("Expected Capacity to be %d, got %d", 9000000, info.DbCapacity)
	}

	if info.HiveParams.KeepAliveInterval != 6000000000 {
		t.Fatalf("Expected HiveParams KeepAliveInterval to be %d, got %d", uint64(6000000000), uint64(info.HiveParams.KeepAliveInterval))
	}

	node.Shutdown()
}

func TestConfigValidate(t *testing.T) {
	for _, c := range []struct {
		cfg *api.Config
		err string
	}{
		{
			cfg: &api.Config{EnsAPIs: []string{
				"/data/testnet/geth.ipc",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"http://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"ws://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"test:/data/testnet/geth.ipc",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"test:ws://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"314159265dD8dbb310642f98f50C066173C1259b@/data/testnet/geth.ipc",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"314159265dD8dbb310642f98f50C066173C1259b@http://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"314159265dD8dbb310642f98f50C066173C1259b@ws://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"test:314159265dD8dbb310642f98f50C066173C1259b@/data/testnet/geth.ipc",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"eth:314159265dD8dbb310642f98f50C066173C1259b@http://127.0.0.1:1234",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"eth:314159265dD8dbb310642f98f50C066173C1259b@ws://127.0.0.1:12344",
			}},
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"eth:",
			}},
			err: "invalid format [tld:][contract-addr@]url for ENS API endpoint configuration \"eth:\": missing url",
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"314159265dD8dbb310642f98f50C066173C1259b@",
			}},
			err: "invalid format [tld:][contract-addr@]url for ENS API endpoint configuration \"314159265dD8dbb310642f98f50C066173C1259b@\": missing url",
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				":314159265dD8dbb310642f98f50C066173C1259",
			}},
			err: "invalid format [tld:][contract-addr@]url for ENS API endpoint configuration \":314159265dD8dbb310642f98f50C066173C1259\": missing tld",
		},
		{
			cfg: &api.Config{EnsAPIs: []string{
				"@/data/testnet/geth.ipc",
			}},
			err: "invalid format [tld:][contract-addr@]url for ENS API endpoint configuration \"@/data/testnet/geth.ipc\": missing contract address",
		},
	} {
		err := validateConfig(c.cfg)
		if c.err != "" && err.Error() != c.err {
			t.Errorf("expected error %q, got %q", c.err, err)
		}
		if c.err == "" && err != nil {
			t.Errorf("unexpected error %q", err)
		}
	}
}

func assignTCPPort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return "", err
	}
	return port, nil
}
