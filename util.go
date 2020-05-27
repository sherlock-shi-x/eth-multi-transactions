package eth_multi_transactions

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

func ToLittleEndianBytes(source uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, source)
	return b
}

func ToLittleEndianHex(source uint64) string {
	return fmt.Sprintf("%x", ToLittleEndianBytes(source))
}

func FromLittleEndianBytes(s []byte) (uint64, error) {
	return binary.LittleEndian.Uint64(s), nil
}

func FromLittleEndianHex(s string) (uint64, error) {
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return FromLittleEndianBytes(decoded)
}

func ToBigEndianBytes(source uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, source)
	return b
}

func ToBigEndianHex(source uint64) string {
	return fmt.Sprintf("%x", ToBigEndianBytes(source))
}

func FromBigEndianBytes(s []byte) (uint64, error) {
	return binary.BigEndian.Uint64(s), nil
}

func FromBigEndianHex(s string) (uint64, error) {
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	return FromBigEndianBytes(decoded)
}
