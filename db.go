package eth_multi_transactions

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	Id       uint64
	Address  string
	Amount   *big.Int
	Nonce    uint64
	Status   uint64
	Hash     string
	Created  uint64
	Modified uint64
}

func (w *WdDB) BatchInsert(objs []*DbWithdrawalObj) error {
	tx, err := w.db.OpenTransaction()
	if err != nil {
		return err
	}

	e := func() error {
		for _, o := range objs {
			id, err := w.GetAndIncreasePrimaryKey(tx)
			if err != nil {
				return err
			}

			_id := ToBigEndianBytes(id)
			_address := []byte(o.Address)
			_amount := o.Amount.Bytes()
			_nonce := ToBigEndianBytes(o.Nonce)
			_status := ToBigEndianBytes(o.Status)
			_hash := []byte(o.Hash)
			_created := ToBigEndianBytes(o.Created)
			_modified := ToBigEndianBytes(o.Modified)

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
		Address:  address,
		Amount:   amount,
		Nonce:    nonce,
		Status:   status,
		Hash:     hash,
		Created:  created,
		Modified: modified,
	}
	var ans []*DbWithdrawalObj
	ans = append(ans, &tmp)
	return w.BatchInsert(ans)
}

func (w *WdDB) GetWdObjById(id uint64) (*DbWithdrawalObj, error) {
	ans := DbWithdrawalObj{Id: id}
	idInBytes := ToBigEndianBytes(id)

	if v, err := w.db.Get(append([]byte("address-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Address = string(v)
	}

	if v, err := w.db.Get(append([]byte("amount-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Amount = big.NewInt(0).SetBytes(v)
	}

	if v, err := w.db.Get(append([]byte("nonce-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Nonce, _ = FromBigEndianBytes(v)
	}

	if v, err := w.db.Get(append([]byte("status-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Status, _ = FromBigEndianBytes(v)
	}

	if v, err := w.db.Get(append([]byte("hash-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Hash = string(v)
	}

	if v, err := w.db.Get(append([]byte("created-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Created, _ = FromBigEndianBytes(v)
	}

	if v, err := w.db.Get(append([]byte("modified-"), idInBytes...), nil); err != nil {
		return nil, err
	} else {
		ans.Modified, _ = FromBigEndianBytes(v)
	}
	return &ans, nil
}

func (w *WdDB) CompareAndSwapStatus(key []byte, from, to uint64) error {
	rawValue, err := w.db.Get(key, nil)
	if err != nil {
		return err
	}

	value, err := FromBigEndianBytes(rawValue)
	if err != nil {
		return err
	}

	if value != from {
		return fmt.Errorf("mismatch value: expected:%v real:%v", from, value)
	}

	return w.db.Put(key, ToBigEndianBytes(to), nil)
}

func (w *WdDB) GetUnhandledRecordsId() ([]uint64, error) {
	itr := w.db.NewIterator(util.BytesPrefix([]byte("status-")), nil)
	initialStatus := ToBigEndianBytes(0)

	var ans []uint64

	for itr.Next() {
		if bytes.Compare(initialStatus, itr.Value()) == 0 {
			v, err := FromBigEndianBytes(itr.Key()[7:])
			if err != nil {
				return nil, err
			}
			ans = append(ans, v)
		}
	}
	itr.Release()
	if err := itr.Error(); err != nil {
		return nil, err
	}
	return ans, nil
}

func (w *WdDB) GetAndIncreasePrimaryKey(tx *leveldb.Transaction) (uint64, error) {
	idRaw, err := tx.Get([]byte("kv-id"), nil)
	if err != nil {
		return 0, err
	}

	id, err := FromBigEndianBytes(idRaw)
	if err != nil {
		return 0, err
	}

	id += 1
	return id, tx.Put([]byte("kv-id"), ToBigEndianBytes(id), nil)
}

func (w *WdDB) GetOrSet(key []byte, initValue []byte) error {
	if ok, err := w.db.Has(key, nil); err != nil {
		return err
	} else if ok {
		return nil
	}

	return w.db.Put(key, initValue, nil)
}
