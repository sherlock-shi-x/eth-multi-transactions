package eth_multi_transactions

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type WdDB struct {
	db *leveldb.DB
}

func NewWithdrawalDB(db *leveldb.DB) *WdDB {
	return &WdDB{
		db: db,
	}
}

func (w *WdDB) GetRawDB() *leveldb.DB {
	return w.db
}

func (w *WdDB) GetOrSet(key []byte, initValue []byte) error {
	if ok, err := w.db.Has(key, nil); err != nil {
		return err
	} else if ok {
		return nil
	}

	return w.db.Put(key, initValue, nil)
}
