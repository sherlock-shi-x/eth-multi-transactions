package main

import (
    "math/big"
    "time"

    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/syndtr/goleveldb/leveldb"

    emt "github.com/haihongs/eth-multi-transactions"
    "github.com/haihongs/eth-multi-transactions/common/logger"
)

var (
    Wei     = big.NewInt(1)
    GWei    = big.NewInt(1e9)
    Ether   = big.NewInt(0).Mul(GWei, GWei) // 1ether = 1e18wei
    ChainID = big.NewInt(1)
)

type dest struct {
    addr    string
    percent *big.Int
    amt     *big.Int // wei based
    memo    string
}

func main() {
    logger.Init(logger.DebugLevel)

    // TODO: flag parse
    path := "./db"
    nodeEndpoint := ""
    addr := ""
    sk := ""
    users := []*dest{
        &dest{addr: "0xaaaaa", amt: big.NewInt(456)},
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

    // initial condition check
    {
        // check kv
        initValue := emt.ToBigEndianBytes(1)

        if err := wdDB.GetOrSet([]byte("kv-id"), initValue); err != nil {
            logger.Fatal("condition check failed", "err", err)
        }

        if err := wdDB.GetOrSet([]byte("kv-nonce"), initValue); err != nil {
            logger.Fatal("condition check failed", "err", err)
        }
    }

    // generate withdrawals
    for _, u := range users {
        if err := wdDB.Insert(u.addr, u.amt, 0, 0, "", uint64(time.Now().Unix()), uint64(time.Now().Unix())); err != nil {
            logger.Info("failed to insert db", "err", err, "addr", u.addr, "amount", u.amt)
            continue
        }
    }

    // main loop
    for {
        if err := handle(wdDB, ethc, addr, sk); err != nil {
            logger.Error("failed to handle it", "err", err)
        }
        time.Sleep(5 * 60 * time.Second)
    }
}

func handle(db *emt.WdDB, ethc *ethclient.Client, addr, sk string) error {
    fromAddr := common.HexToAddress(addr)
    prvKey, err := crypto.HexToECDSA(sk)
    if err != nil {
        return err
    }

    // handle
    ids, err := db.GetUnhandledRecordsId()
    if err != nil {
        return err
    }

    logger.Info("start handling")
    for _, id := range ids {
        key := append([]byte("status-"), emt.ToBigEndianBytes(id)...)
        if err := db.CompareAndSwapStatus(key, 0, 1); err != nil {
            logger.Error("failed to CAS status", "err", err, "id", id)
            continue
        }

        obj, err := db.GetWdObjById(id)
        if err != nil {
            logger.Error("failed to get wd obj", "err", err, "id", id)
            continue
        }

        txId, err := emt.SendEthTransaction(obj, ethc, fromAddr, prvKey, ChainID)
        if err != nil {
            logger.Error("failed to send eth transaction", "err", err, "id", id)
            continue
        }
        logger.Info("broadcast succeed", "txid", txId)

        if err := emt.PollingTransaction(ethc, txId, 3, 60*time.Minute); err != nil {
            logger.Error("failed to confirm eth transaction", "err", err, "id", id)
            continue
        }

        if err := db.CompareAndSwapStatus(key, 1, 2); err != nil {
            logger.Error("failed to CAS status", "err", err, "id", id)
            continue
        }
    }
    logger.Info("finish handling")
    return nil
}
