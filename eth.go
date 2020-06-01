package eth_multi_transactions

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func GetBalance(c *ethclient.Client, addr string) (*big.Int, error) {
	return c.BalanceAt(context.Background(), common.HexToAddress(addr), nil)
}
