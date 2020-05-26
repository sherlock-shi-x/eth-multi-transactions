package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/syndtr/goleveldb/leveldb"

	emt "github.com/haihongs/eth-multi-transactions"
	"github.com/haihongs/eth-multi-transactions/common/logger"
)

func main() {
	logger.Init(logger.DebugLevel)

	path := "./db"
	nodeEndpoint := ""
	addr := ""

	// register db
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		logger.Fatal("failed to init leveldb", "dir", path, "err", err)
	}
	defer db.Close()

	wdDB := emt.NewWithdrawalDB(db)

	// register ethclient
	ethc, err := ethclient.Dial(nodeEndpoint)
	if err != nil {
		logger.Fatal("failed to init ethclient", "err", err)
	}

	for {
		if err := handle(wdDB, ethc, addr); err != nil {
			logger.Error("failed to handle it", "err", err)
		}
		time.Sleep(60 * time.Second)
	}
}

func handle(db *emt.WdDB, ethc *ethclient.Client, addr string) error {
	// check kv
	initValue := emt.ToLittleEndianBytes(1)

	if err := db.GetOrSet([]byte("id"), initValue); err != nil {
		return err
	}

	if err := db.GetOrSet([]byte("nonce"), initValue); err != nil {
		return err
	}

	// calibrate nonce
	var dbNonce uint64
	if n, err := db.GetRawDB().Get([]byte("nonce"), nil); err != nil {
		return err
	} else {
		if nn, err1 := emt.FromLittleEndianBytes(n); err1 != nil {
			return err1
		} else {
			dbNonce = nn
		}
	}

	var blockchainNonce uint64
	if b, err := ethc.NonceAt(context.Background(), common.HexToAddress(addr), nil); err != nil {
		return err
	} else {
		blockchainNonce = b
	}

	if dbNonce < blockchainNonce {
		return fmt.Errorf("wrong nonce count, db: %v blockchain: %v", dbNonce, blockchainNonce)
	}

	return nil
}
