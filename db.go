package eth_multi_transactions

import (
	"math/big"

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

func (w *WdDB) Insert(
	id uint64,
	address string,
	amount *big.Int,
	nonce uint64,
	status uint64,
	hash string,
	created uint64,
	modified uint64,
) error {
	_id := ToBigEndianBytes(id)
	_address := []byte(address)
	_amount := amount.Bytes()
	_nonce := ToBigEndianBytes(nonce)
	_status := ToBigEndianBytes(status)
	_hash := []byte(hash)
	_created := ToBigEndianBytes(created)
	_modified := ToBigEndianBytes(modified)

	tx, err := w.db.OpenTransaction()
	if err != nil {
		return err
	}

	if err := tx.Put(append([]byte("address-"), _id...), _address, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("amount-"), _id...), _amount, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("nonce-"), _id...), _nonce, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("status-"), _id...), _status, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("hash-"), _id...), _hash, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("created-"), _id...), _created, nil); err != nil {
		return err
	}
	if err := tx.Put(append([]byte("modified-"), _id...), _modified, nil); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *WdDB) GetNewNonce() uint64 {
	return 0
}

func (w *WdDB) GetOrSet(key []byte, initValue []byte) error {
	if ok, err := w.db.Has(key, nil); err != nil {
		return err
	} else if ok {
		return nil
	}

	return w.db.Put(key, initValue, nil)
}
