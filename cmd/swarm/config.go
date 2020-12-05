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
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"unicode"

	"github.com/ethereum/go-ethereum/common"
	cli "gopkg.in/urfave/cli.v1"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/naoina/toml"

	bzzapi "github.com/ethersphere/swarm/api"
	"github.com/ethersphere/swarm/network"
)

var (
	//flag definition for the dumpconfig command
	DumpConfigCommand = cli.Command{
		Action:      utils.MigrateFlags(dumpConfig),
		Name:        "dumpconfig",
		Usage:       "Show configuration values",
		ArgsUsage:   "",
		Flags:       app.Flags,
		Category:    "MISCELLANEOUS COMMANDS",
		Description: `The dumpconfig command shows configuration values.`,
	}

	//flag definition for the config file command
	SwarmTomlConfigPathFlag = cli.StringFlag{
		Name:  "config",
		Usage: "TOML configuration file",
	}
)

//constants for environment variables
const (
	SwarmEnvAccount                 = "SWARM_ACCOUNT"
	SwarmEnvBzzKeyHex               = "SWARM_BZZ_KEY_HEX"
	SwarmEnvListenAddr              = "SWARM_LISTEN_ADDR"
	SwarmEnvPort                    = "SWARM_PORT"
	SwarmEnvNetworkID               = "SWARM_NETWORK_ID"
	SwarmEnvChequebookAddr          = "SWARM_CHEQUEBOOK_ADDR"
	SwarmEnvChequebookFactoryAddr   = "SWARM_SWAP_CHEQUEBOOK_FACTORY_ADDR"
	SwarmEnvInitialDeposit          = "SWARM_INITIAL_DEPOSIT"
	SwarmEnvSwapEnable              = "SWARM_SWAP_ENABLE"
	SwarmEnvSwapBackendURL          = "SWARM_SWAP_BACKEND_URL"
	SwarmEnvSwapPaymentThreshold    = "SWARM_SWAP_PAYMENT_THRESHOLD"
	SwarmEnvSwapDisconnectThreshold = "SWARM_SWAP_DISCONNECT_THRESHOLD"
	SwarmSyncMode                   = "SWARM_SYNC_MODE"
	SwarmEnvSwapLogPath             = "SWARM_SWAP_LOG_PATH"
	SwarmEnvSyncUpdateDelay         = "SWARM_ENV_SYNC_UPDATE_DELAY"
	SwarmEnvMaxStreamPeerServers    = "SWARM_ENV_MAX_STREAM_PEER_SERVERS"
	SwarmEnvLightNodeEnable         = "SWARM_LIGHT_NODE_ENABLE"
	SwarmEnvDeliverySkipCheck       = "SWARM_DELIVERY_SKIP_CHECK"
	SwarmEnvENSAPI                  = "SWARM_ENS_API"
	SwarmEnvRNSAPI                  = "SWARM_RNS_API"
	SwarmEnvENSAddr                 = "SWARM_ENS_ADDR"
	SwarmEnvCORS                    = "SWARM_CORS"
	SwarmEnvBootnodes               = "SWARM_BOOTNODES"
	SwarmEnvPSSEnable               = "SWARM_PSS_ENABLE"
	SwarmEnvStorePath               = "SWARM_STORE_PATH"
	SwarmEnvStoreCapacity           = "SWARM_STORE_CAPACITY"
	SwarmEnvStoreCacheCapacity      = "SWARM_STORE_CACHE_CAPACITY"
	SwarmEnvBootnodeMode            = "SWARM_BOOTNODE_MODE"
	SwarmEnvNATInterface            = "SWARM_NAT_INTERFACE"
	SwarmAccessPassword             = "SWARM_ACCESS_PASSWORD"
	SwarmAutoDefaultPath            = "SWARM_AUTO_DEFAULTPATH"
	SwarmGlobalstoreAPI             = "SWARM_GLOBALSTORE_API"
	GethEnvDataDir                  = "GETH_DATADIR"
)

// These settings ensure that TOML keys use the same names as Go struct fields.
var tomlSettings = toml.Config{
	NormFieldName: func(rt reflect.Type, key string) string {
		return key
	},
	FieldToKey: func(rt reflect.Type, field string) string {
		return field
	},
	MissingField: func(rt reflect.Type, field string) error {
		link := ""
		if unicode.IsUpper(rune(rt.Name()[0])) && rt.PkgPath() != "main" {
			link = fmt.Sprintf(", check github.com/ethersphere/swarm/api/config.go for available fields")
		}
		return fmt.Errorf("field '%s' is not defined in %s%s", field, rt.String(), link)
	},
}

//before booting the swarm node, build the configuration
func buildConfig(ctx *cli.Context) (config *bzzapi.Config, err error) {
	//start by creating a default config
	config = bzzapi.NewConfig()
	//first load settings from config file (if provided)
	config, err = configFileOverride(config, ctx)
	if err != nil {
		return nil, err
	}
	//override settings provided by flags
	config = flagsOverride(config, ctx)
	//validate configuration parameters
	err = validateConfig(config)

	return
}

//finally, after the configuration build phase is finished, initialize
func initSwarmNode(config *bzzapi.Config, stack *node.Node, ctx *cli.Context, nodeconfig *node.Config) error {
	//get the account for the provided swarm account
	var prvkey *ecdsa.PrivateKey
	config.BzzAccount, prvkey = getOrCreateAccount(ctx, stack)
	//set the resolved config path (geth --datadir)
	config.Path = expandPath(stack.InstanceDir())
	//finally, initialize the configuration
	err := config.Init(prvkey, nodeconfig.NodeKey())
	if err != nil {
		return err
	}
	//configuration phase completed here
	log.Debug("Starting Swarm with the following parameters:")
	//after having created the config, print it to screen
	log.Debug(printConfig(config))
	return nil
}

//configFileOverride overrides the current config with the config file, if a config file has been provided
func configFileOverride(config *bzzapi.Config, ctx *cli.Context) (*bzzapi.Config, error) {
	var err error

	//only do something if the -config flag has been set
	if ctx.GlobalIsSet(SwarmTomlConfigPathFlag.Name) {
		var filepath string
		if filepath = ctx.GlobalString(SwarmTomlConfigPathFlag.Name); filepath == "" {
			utils.Fatalf("Config file flag provided with invalid file path")
		}
		var f *os.File
		f, err = os.Open(filepath)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		//decode the TOML file into a Config struct
		//note that we are decoding into the existing defaultConfig;
		//if an entry is not present in the file, the default entry is kept
		err = tomlSettings.NewDecoder(f).Decode(&config)
		// Add file name to errors that have a line number.
		if _, ok := err.(*toml.LineError); ok {
			err = errors.New(filepath + ", " + err.Error())
		}
	}
	return config, err
}

// flagsOverride overrides the current config with whatever is provided through flags (cli or env vars)
// most values are not allowed a zero value (empty string), if not otherwise noted
func flagsOverride(currentConfig *bzzapi.Config, ctx *cli.Context) *bzzapi.Config {
	if keyid := ctx.GlobalString(SwarmAccountFlag.Name); keyid != "" {
		currentConfig.BzzAccount = keyid
	}
	if chbookaddr := ctx.GlobalString(SwarmSwapChequebookAddrFlag.Name); chbookaddr != "" {
		currentConfig.Contract = common.HexToAddress(chbookaddr)
	}
	if chequebookFactoryAddress := ctx.GlobalString(SwarmSwapChequebookFactoryFlag.Name); chequebookFactoryAddress != "" {
		currentConfig.SwapChequebookFactory = common.HexToAddress(chequebookFactoryAddress)
	}
	networkid := ctx.GlobalUint64(SwarmNetworkIdFlag.Name)
	if networkid != 0 && networkid != network.DefaultNetworkID {
		currentConfig.NetworkID = networkid
	}
	if ctx.GlobalIsSet(utils.DataDirFlag.Name) {
		if datadir := ctx.GlobalString(utils.DataDirFlag.Name); datadir != "" {
			currentConfig.Path = expandPath(datadir)
		}
	}
	bzzport := ctx.GlobalString(SwarmPortFlag.Name)
	if len(bzzport) > 0 {
		currentConfig.Port = bzzport
	}
	if bzzaddr := ctx.GlobalString(SwarmListenAddrFlag.Name); bzzaddr != "" {
		currentConfig.ListenAddr = bzzaddr
	}
	if ctx.GlobalIsSet(SwarmSwapEnabledFlag.Name) {
		currentConfig.SwapEnabled = true
	}
	if swapBackendURL := ctx.GlobalString(SwarmSwapBackendURLFlag.Name); swapBackendURL != "" {
		currentConfig.SwapBackendURL = swapBackendURL
	}
	if swapLogPath := ctx.GlobalString(SwarmSwapLogPathFlag.Name); currentConfig.SwapEnabled && swapLogPath != "" {
		currentConfig.SwapLogPath = swapLogPath
	}
	if initialDepo := ctx.GlobalUint64(SwarmSwapInitialDepositFlag.Name); initialDepo != 0 {
		currentConfig.SwapInitialDeposit = initialDepo
	}
	if paymentThreshold := ctx.GlobalUint64(SwarmSwapPaymentThresholdFlag.Name); paymentThreshold != 0 {
		currentConfig.SwapPaymentThreshold = paymentThreshold
	}
	if disconnectThreshold := ctx.GlobalUint64(SwarmSwapDisconnectThresholdFlag.Name); disconnectThreshold != 0 {
		currentConfig.SwapDisconnectThreshold = disconnectThreshold
	}
	if ctx.GlobalIsSet(SwarmSyncModeFlag.Name) {
		currentConfig.SyncEnabled, currentConfig.PushSyncEnabled = syncModeParse(ctx.GlobalString(SwarmSyncModeFlag.Name))
	}
	if d := ctx.GlobalDuration(SwarmSyncUpdateDelay.Name); d > 0 {
		currentConfig.SyncUpdateDelay = d
	}
	// any value including 0 is acceptable
	currentConfig.MaxStreamPeerServers = ctx.GlobalInt(SwarmMaxStreamPeerServersFlag.Name)
	if ctx.GlobalIsSet(SwarmLightNodeEnabled.Name) {
		currentConfig.LightNodeEnabled = true
	}
	if ctx.GlobalIsSet(SwarmDeliverySkipCheckFlag.Name) {
		currentConfig.DeliverySkipCheck = true
	}
	if ctx.GlobalIsSet(EnsAPIFlag.Name) {
		ensAPIs := ctx.GlobalStringSlice(EnsAPIFlag.Name)
		// preserve backward compatibility to disable ENS with --ens-api=""
		if len(ensAPIs) == 1 && ensAPIs[0] == "" {
			ensAPIs = nil
		}
		for i := range ensAPIs {
			ensAPIs[i] = expandPath(ensAPIs[i])
		}
		currentConfig.EnsAPIs = ensAPIs
	}
	if ctx.GlobalIsSet(RnsAPIFlag.Name) {
		currentConfig.RnsAPI = ctx.GlobalString(RnsAPIFlag.Name)
	}
	if cors := ctx.GlobalString(CorsStringFlag.Name); cors != "" {
		currentConfig.Cors = cors
	}
	if storePath := ctx.GlobalString(SwarmStorePath.Name); storePath != "" {
		currentConfig.ChunkDbPath = storePath
	}
	if storeCapacity := ctx.GlobalUint64(SwarmStoreCapacity.Name); storeCapacity != 0 {
		currentConfig.DbCapacity = storeCapacity
	}
	if ctx.GlobalIsSet(SwarmStoreCacheCapacity.Name) {
		currentConfig.CacheCapacity = ctx.GlobalUint(SwarmStoreCacheCapacity.Name)
	}
	if ctx.GlobalIsSet(SwarmBootnodeModeFlag.Name) {
		currentConfig.BootnodeMode = ctx.GlobalBool(SwarmBootnodeModeFlag.Name)
	}
	if ctx.GlobalIsSet(SwarmDisableAutoConnectFlag.Name) {
		currentConfig.DisableAutoConnect = ctx.GlobalBool(SwarmDisableAutoConnectFlag.Name)
	}
	if ctx.GlobalIsSet(SwarmGlobalStoreAPIFlag.Name) {
		currentConfig.GlobalStoreAPI = ctx.GlobalString(SwarmGlobalStoreAPIFlag.Name)
	}
	if ctx.GlobalBool(SwarmEnablePinningFlag.Name) {
		currentConfig.EnablePinning = true
	}
	return currentConfig
}

func syncModeParse(s string) (pullSync, pushSync bool) {
	switch s {
	case "pull":
		return true, false
	case "push":
		return false, true
	case "all":
		return true, true
	case "none":
		return false, false
	default:
		utils.Fatalf("unknown cli flag %s value: %v", SwarmSyncModeFlag.Name, s)
		return
	}
}

// dumpConfig is the dumpconfig command.
// writes a default config to STDOUT
func dumpConfig(ctx *cli.Context) error {
	cfg, err := buildConfig(ctx)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("Uh oh - dumpconfig triggered an error %v", err))
	}
	comment := ""
	out, err := tomlSettings.Marshal(&cfg)
	if err != nil {
		return err
	}
	io.WriteString(os.Stdout, comment)
	os.Stdout.Write(out)
	return nil
}

//validate configuration parameters
func validateConfig(cfg *bzzapi.Config) (err error) {
	for _, ensAPI := range cfg.EnsAPIs {
		if ensAPI != "" {
			if err := validateEnsAPIs(ensAPI); err != nil {
				return fmt.Errorf("invalid format [tld:][contract-addr@]url for ENS API endpoint configuration %q: %v", ensAPI, err)
			}
		}
	}
	return nil
}

//validate EnsAPIs configuration parameter
func validateEnsAPIs(s string) (err error) {
	// missing contract address
	if strings.HasPrefix(s, "@") {
		return errors.New("missing contract address")
	}
	// missing url
	if strings.HasSuffix(s, "@") {
		return errors.New("missing url")
	}
	// missing tld
	if strings.HasPrefix(s, ":") {
		return errors.New("missing tld")
	}
	// missing url
	if strings.HasSuffix(s, ":") {
		return errors.New("missing url")
	}
	return nil
}

//print a Config as string
func printConfig(config *bzzapi.Config) string {
	out, err := tomlSettings.Marshal(&config)
	if err != nil {
		return fmt.Sprintf("Something is not right with the configuration: %v", err)
	}
	return string(out)
}
