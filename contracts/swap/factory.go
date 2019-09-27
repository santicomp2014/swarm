package swap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	contractFactory "github.com/ethersphere/go-sw3/contracts-v0-1-1/simpleswapfactory"
)

var (
	// ErrNotDeployedByFactory is given when a contract was not deployed by the factory
	ErrNotDeployedByFactory = errors.New("not deployed by factory")

	// Deployments maps from network ids to deployed contract factories
	Deployments = map[uint64]common.Address{
		// Ropsten
		3: common.HexToAddress("0x2e9C43E186eaF4fee10799d67e75f8CFc5BA3a0c"),
	}
)

type simpleSwapFactory struct {
	instance *contractFactory.SimpleSwapFactory
	address  common.Address
	backend  Backend
}

// SimpleSwapFactory interface defines the methods available for a factory contract for SimpleSwap
type SimpleSwapFactory interface {
	// DeploySimpleSwap deploys a new SimpleSwap contract from the factory and returns the ready to use Contract abstraction
	DeploySimpleSwap(auth *bind.TransactOpts, issuer common.Address, defaultHardDepositTimeoutDuration *big.Int) (Contract, error)
	// VerifyContract verifies that the supplied address was deployed by this factory
	VerifyContract(address common.Address) error
	// VerifySelf verifies that this is a valid factory on the network
	VerifySelf() error
}

// FactoryAt creates a SimpleSwapFactory instance for the given address and backend
func FactoryAt(address common.Address, backend Backend) (SimpleSwapFactory, error) {
	simple, err := contractFactory.NewSimpleSwapFactory(address, backend)
	if err != nil {
		return nil, err
	}
	c := simpleSwapFactory{instance: simple, address: address, backend: backend}
	return c, err
}

// FactoryAddressForNetwork gets the default factory address for a given network id
func FactoryAddressForNetwork(networkID uint64) (common.Address, error) {
	address, ok := Deployments[networkID]
	if !ok {
		return common.Address{}, fmt.Errorf("no known factory contract for ethereum network %d", networkID)
	}
	return address, nil
}

// VerifySelf verifies that this is a valid factory on the network
func (sf simpleSwapFactory) VerifySelf() error {
	code, err := sf.backend.CodeAt(context.Background(), sf.address, nil)
	if err != nil {
		return err
	}

	referenceCode := common.FromHex(contractFactory.SimpleSwapFactoryDeployedCode)
	if !bytes.Equal(code, referenceCode) {
		return errors.New("not a valid factory contract")
	}

	return nil
}

// DeploySimpleSwap deploys a new SimpleSwap contract from the factory and returns the ready to use Contract abstraction
func (sf simpleSwapFactory) DeploySimpleSwap(auth *bind.TransactOpts, issuer common.Address, defaultHardDepositTimeoutDuration *big.Int) (Contract, error) {
	// for some reason the automatic gas estimation is too low
	auth.GasLimit = 1700000
	tx, err := sf.instance.DeploySimpleSwap(auth, issuer, defaultHardDepositTimeoutDuration)
	if err != nil {
		return nil, err
	}

	receipt, err := WaitFunc(auth, sf.backend, tx)
	if err != nil {
		return nil, err
	}

	// we iterate through the logs until we find the SimpleSwapDeployed event which contains the address of the new SimpleSwap contract
	address := common.Address{}
	for _, log := range receipt.Logs {
		if log.Address != sf.address {
			continue
		}
		if event, err := sf.instance.ParseSimpleSwapDeployed(*log); err == nil {
			address = event.ContractAddress
			break
		}
	}
	if (address == common.Address{}) {
		return nil, errors.New("contract deployment failed")
	}

	simpleSwap, err := InstanceAt(address, sf.backend)
	if err != nil {
		return nil, err
	}

	return simpleSwap, nil
}

// VerifyContract verifies that the supplied address was deployed by this factory
func (sf simpleSwapFactory) VerifyContract(address common.Address) error {
	isDeployed, err := sf.instance.DeployedContracts(&bind.CallOpts{}, address)
	if err != nil {
		return err
	}
	if !isDeployed {
		return ErrNotDeployedByFactory
	}
	return nil
}
