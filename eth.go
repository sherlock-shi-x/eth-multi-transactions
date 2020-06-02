package eth_multi_transactions

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/haihongs/eth-multi-transactions/common/logger"
)

var (
	Wei   = big.NewInt(1)
	GWei  = big.NewInt(1e9)
	Ether = big.NewInt(0).Mul(GWei, GWei) // 1ether = 1e18wei
)

func GetBalance(c *ethclient.Client, addr string) (*big.Int, error) {
	return c.BalanceAt(context.Background(), common.HexToAddress(addr), nil)
}

func PollingTransaction(ethc *ethclient.Client, txid string, threshold int64, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	ticker := time.NewTicker(30 * time.Second)
	cnt := int64(0)

	for range ticker.C {
		if time.Now().After(end) {
			return fmt.Errorf("polling timeout, txid: %s", txid)
		}

		_, isPending, err := ethc.TransactionByHash(context.Background(), common.HexToHash(txid))
		if err != nil {
			logger.Error("failed to get transaction by hash", "err", err)
			continue
		}

		if isPending {
			continue
		}

		cnt++
		if cnt >= threshold {
			return nil
		}
	}
	return nil
}

func SendEthTransaction(
	obj *DbWithdrawalObj,
	ethc *ethclient.Client,
	fromAddr common.Address,
	prvKey *ecdsa.PrivateKey,
) (string, error) {
	// build tx
	ctx := context.Background()
	nonce, err := ethc.NonceAt(ctx, fromAddr, nil)
	if err != nil {
		return "", err
	}

	gasPrice, err := ethc.SuggestGasPrice(ctx)
	if err != nil {
		return "", err
	}

	if balance, err := ethc.BalanceAt(ctx, fromAddr, nil); err != nil {
		return "", err
	} else {
		if balance.Cmp(obj.Amount) <= 0 {
			return "", fmt.Errorf("not enough balance, need: %v real: %v", obj.Amount, balance)
		}
	}

	gasPrice.Add(gasPrice, big.NewInt(0).Mul(big.NewInt(5), GWei))
	toAddr := common.HexToAddress(obj.Address)
	tx := types.NewTransaction(nonce, toAddr, obj.Amount, uint64(21000), gasPrice, []byte{})
	signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, prvKey)
	if err != nil {
		return "", err
	}

	// broadcast
	if err = ethc.SendTransaction(ctx, signedTx); err != nil {
		return "", err
	}

	return signedTx.Hash().Hex(), nil
}
