package eth_multi_transactions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/haihongs/eth-multi-transactions/common/logger"
)

func TestScanningDB(t *testing.T) {
	t.Skip("skip in CI")

	db := testOpenDB()
	defer db.db.Close()

	itr := db.db.NewIterator(nil, nil)
	defer itr.Release()

	for itr.Next() {
		fmt.Printf("itr: %+v %+v %+v\n", string(itr.Key()), itr.Key(), itr.Value())
		//if itr.Value() != nil {
		//	v, err := FromBigEndianBytes(itr.Value())
		//	if err != nil {
		//		logger.Error("failed to decode value", "err", err)
		//		continue
		//	}
		//	fmt.Printf("%+v %+v\n", string(itr.Key()), v)
		//}
	}
}

func TestModifyDB(t *testing.T) {
	t.Skip("skip in CI")

	db := testOpenDB()
	defer db.db.Close()

	key := []byte("kv-Nonce")
	value := ToBigEndianBytes(2160)

	err := db.db.Put(key, value, nil)
	assert.NoError(t, err)
}

func testOpenDB() *WdDB {
	logger.Init(logger.DebugLevel)

	path := "./cmd/db"
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		logger.Fatal("failed to init db", "err", err)
	}
	return NewWithdrawalDB(db)
}

func TestWdDB_GetUnhandledRecordsId(t *testing.T) {
	t.Skip("skip in CI")

	db := testOpenDB()
	defer db.db.Close()

	ans, err := db.GetUnhandledRecordsId()
	assert.NoError(t, err)
	for _, v := range ans {
		fmt.Printf("%v\n", v)
	}
}

func TestWdDB_GetWdObjById(t *testing.T) {
	t.Skip("skip in CI")

	db := testOpenDB()
	defer db.db.Close()

	ans, err := db.GetWdObjById(20)
	assert.NoError(t, err)

	fmt.Printf("%+v %v\n", ans, ans.Amount.String())
}
