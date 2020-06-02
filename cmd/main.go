package main

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/robfig/cron/v3"
	"github.com/syndtr/goleveldb/leveldb"

	emt "github.com/haihongs/eth-multi-transactions"
	"github.com/haihongs/eth-multi-transactions/common/logger"
)

var (
	Wei   = big.NewInt(1)
	GWei  = big.NewInt(1e9)
	Ether = big.NewInt(0).Mul(GWei, GWei) // 1ether = 1e18wei
)

type dest struct {
	addr    string
	percent *big.Int
	amt     *big.Int
}

func main() {
	logger.Init(logger.DebugLevel)

	// TODO: flag parse
	path := "./db"
	nodeEndpoint := ""
	addr := "0xa"
	sk := "0x0"
	users := []*dest{
		&dest{addr: "0x1f3b29aE0d5eDAe9bb148537D4ED2B12BEdDf8B3", percent: big.NewInt(10)},
		&dest{addr: "0x2f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x3f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x4f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x5f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x6f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x7f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(10)},
		&dest{addr: "0x8f3b29aE0d5eDAe9bdb148537D4ED2B12BEdDf8B", percent: big.NewInt(30)},
	}

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

	// generate withdrawals
	c := cron.New()
	if _, err := c.AddFunc("@every 24h", func() { generateWithdrawals(wdDB, ethc, addr, users) }); err != nil {
		logger.Fatal("failed to init cron", "err", err)
	}
	c.Start()

	// main loop
	for {
		if err := handle(wdDB, ethc, addr, sk); err != nil {
			logger.Error("failed to handle it", "err", err)
		}
		time.Sleep(60 * time.Second)
	}
}

func handle(db *emt.WdDB, ethc *ethclient.Client, addr, sk string) error {
	fromAddr := common.HexToAddress(addr)
	prvKey, err := crypto.HexToECDSA(sk)
	if err != nil {
		return err
	}

	// TODO: move check into main part
	{
		// check kv
		initValue := emt.ToBigEndianBytes(1)

		if err := db.GetOrSet([]byte("kv-id"), initValue); err != nil {
			return err
		}

		if err := db.GetOrSet([]byte("kv-nonce"), initValue); err != nil {
			return err
		}

		// calibrate nonce
		var dbNonce uint64
		if n, err := db.GetRawDB().Get([]byte("kv-nonce"), nil); err != nil {
			return err
		} else {
			if nn, err1 := emt.FromBigEndianBytes(n); err1 != nil {
				return err1
			} else {
				dbNonce = nn
			}
		}

		var blockchainNonce uint64
		if b, err := ethc.NonceAt(context.Background(), fromAddr, nil); err != nil {
			return err
		} else {
			blockchainNonce = b
		}

		if dbNonce < blockchainNonce {
			return fmt.Errorf("wrong nonce count, db: %v blockchain: %v", dbNonce, blockchainNonce)
		}
	}

	// handle
	ids, err := db.GetUnhandledRecordsId()
	if err != nil {
		return err
	}

	for _, id := range ids {
		if err := db.CompareAndSwapStatus(append([]byte("status-"), emt.ToBigEndianBytes(id)...), 0, 1); err != nil {
			logger.Error("failed to CAS status", "err", err, "id", id)
			continue
		}

		obj, err := db.GetWdObjById(id)
		if err != nil {
			logger.Error("failed to get wd obj", "err", err, "id", id)
			continue
		}

		txId, err := emt.SendEthTransaction(obj, ethc, fromAddr, prvKey)
		if err != nil {
			logger.Error("failed to send eth transaction", "err", err, "id", id)
			continue
		}

		if err := emt.PollingTransaction(ethc, txId, 3, 40*time.Minute); err != nil {
			logger.Error("failed to confirm eth transaction", "err", err, "id", id)
			continue
		}
	}
	return nil
}

func generateWithdrawals(wdDB *emt.WdDB, ethc *ethclient.Client, addr string, users []*dest) {
	// retry at most 5 times
	for i := 0; i < 5; i++ {
		// get balance
		balance, err := emt.GetBalance(ethc, addr)
		if err != nil {
			logger.Error("failed to get balance", "err", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// keep 1ether to pay the network fee
		if big.NewInt(0).Div(balance, Ether).Uint64() < 1 {
			logger.Info("not enough balance")
			return
		}

		balance.Sub(balance, Ether)

		// generate records
		total := big.NewInt(0)
		for _, u := range users {
			total.Add(total, u.percent)
		}

		// balance * percent / total
		for _, u := range users {
			u.amt = big.NewInt(0).Mul(balance, u.percent)
			u.amt.Div(u.amt, total)
			if err := wdDB.Insert(u.addr, u.amt, 0, 0, "", uint64(time.Now().Unix()), uint64(time.Now().Unix())); err != nil {
				logger.Info("failed to insert db", "err", err)
				continue
			}
		}

		logger.Info("succeed to generate withdrawals")

		// TODO: set nonce
		// TODO: dingding notification
		return
	}
}
