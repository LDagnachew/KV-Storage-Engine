package hash_indexes

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
)

/*
	V1 Design:
	| Key Length | Value Length | Key | Value |
	============================================
	|   4 bytes  |    4 bytes   |  x  |   x   |
	--------------------------------------------
*/
// TODO: In-Memory HashMap
type HashIndex struct {
	f     *os.File
	index map[string]int64 // key -> offset in log
}

func NewHashIndex() (*HashIndex, error) {
	file, err := os.OpenFile("dataseg", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &HashIndex{
		f:     file,
		index: make(map[string]int64),
	}, nil
}

func (h *HashIndex) Put(key, value []byte) error {
	lastByte, err := h.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// KEY_LEN, VAL_LEN
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)
	log.Printf("Key Length is Currently %d\n", uint32(len(key)))
	log.Printf("Value Length is Currently %d\n", uint32(len(value)))
	// place using little endian
	binary.BigEndian.PutUint32(keylen32, uint32(len(key)))
	binary.BigEndian.PutUint32(vallen32, uint32(len(value)))
	// write these to dataseg
	h.index[string(key)] = lastByte
	log.Printf("lastByte currently is %d\n", lastByte)
	h.f.WriteAt(keylen32, lastByte)
	h.f.WriteAt(vallen32, lastByte+4)
	// now write KV pair
	h.f.WriteAt(key, lastByte+8)
	h.f.WriteAt(value, lastByte+int64(8)+int64(len(key)))
	h.f.Sync()
	return nil
}

func (h *HashIndex) Get(key []byte) ([]byte, error) {
	// TODO: Fetch from map, read from byte offset
	off, ok := h.index[string(key)]
	if !ok {
		return []byte{}, errors.New("key does not exist")
	}
	// Seek, then read
	// Skip Key, Read value len, go to position lastByte + 8 + key_len, read value & return
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)

	h.f.ReadAt(keylen32, off)
	h.f.ReadAt(vallen32, off+4)
	// turn to ints
	k_len := binary.BigEndian.Uint32(keylen32)
	v_len := binary.BigEndian.Uint32(vallen32)
	log.Printf("k_len=%d v_len=%d\n", k_len, v_len)

	value := make([]byte, v_len)
	h.f.ReadAt(value, off+8+int64(k_len))
	return value, nil

}

func (h *HashIndex) Close() error {
	return h.f.Close()
}
