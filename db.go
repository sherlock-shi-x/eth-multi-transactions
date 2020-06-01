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

type DbWithdrawalObj struct {
	address  string
	amount   *big.Int
	nonce    uint64
	status   uint64
	hash     string
	created  uint64
	modified uint64
}

func (w *WdDB) BatchInsert(objs []*DbWithdrawalObj) error {
	tx, err := w.db.OpenTransaction()
	if err != nil {
		return err
	}

	e := func() error {
		for _, o := range objs {
			id, err := w.GetAndIncreasePrimaryKey()
			if err != nil {
				return err
			}

			_id := ToBigEndianBytes(id)
			_address := []byte(o.address)
			_amount := o.amount.Bytes()
			_nonce := ToBigEndianBytes(o.nonce)
			_status := ToBigEndianBytes(o.status)
			_hash := []byte(o.hash)
			_created := ToBigEndianBytes(o.created)
			_modified := ToBigEndianBytes(o.modified)

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
		}
		return nil
	}()

	if e != nil {
		tx.Discard()
		return e
	}
	return tx.Commit()
}

func (w *WdDB) Insert(
	address string,
	amount *big.Int,
	nonce uint64,
	status uint64,
	hash string,
	created uint64,
	modified uint64,
) error {
	tmp := DbWithdrawalObj{
		address:  address,
		amount:   amount,
		nonce:    nonce,
		status:   status,
		hash:     hash,
		created:  created,
		modified: modified,
	}
	var ans []*DbWithdrawalObj
	ans = append(ans, &tmp)
	return w.BatchInsert(ans)
}

func (w *WdDB) GetNewNonce() uint64 {
	return 0
}

func (w *WdDB) GetAndIncreasePrimaryKey() (uint64, error) {
	idRaw, err := w.db.Get([]byte("kv-id"), nil)
	if err != nil {
		return 0, err
	}

	id, err := FromBigEndianBytes(idRaw)
	if err != nil {
		return 0, err
	}

	id += 1
	return id, w.db.Put([]byte("kv-id"), ToBigEndianBytes(id), nil)
}

func (w *WdDB) GetOrSet(key []byte, initValue []byte) error {
	if ok, err := w.db.Has(key, nil); err != nil {
		return err
	} else if ok {
		return nil
	}

	return w.db.Put(key, initValue, nil)
}
