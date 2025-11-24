package main

import (
	"encoding/binary"
	"errors"
	"os"
)

// TODO: In-Memory HashMap
type HashIndex struct {
	f     *os.File
	index map[string]int64 // key -> offset in log
}

func NewHashIndex() (*HashIndex, error) {
	file, err := os.OpenFile("dataseg", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &HashIndex{
		f:     file,
		index: make(map[string]int64),
	}, nil
}

func (h *HashIndex) Put(key, value []byte) error {
	lastByte, err := h.f.Seek(0, 2)
	if err != nil {
		return err
	}
	// KEY_LEN, VAL_LEN
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)
	// place using little endian
	binary.BigEndian.PutUint32(keylen32, uint32(len(key)))
	binary.BigEndian.PutUint32(vallen32, uint32(len(value)))
	// write these to dataseg
	h.index[string(key)] = lastByte
	h.f.WriteAt(keylen32, lastByte)
	h.f.WriteAt(vallen32, lastByte+4)
	// now write KV pair
	h.f.WriteAt(key, lastByte+8)
	h.f.WriteAt(value, lastByte+int64(8)+int64(len(key)))
	return nil
}

func (h *HashIndex) Get(key []byte) ([]byte, error) {
	// TODO: Fetch from map, read from byte offset
	off, ok := h.index[string(key)]
	if !ok {
		return []byte{}, errors.New("This Key Doesn't Exist")
	}
	// Seek, then read
	// Skip Key, Read value len, go to position lastByte + 8 + key_len, read value & return
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)

	h.f.ReadAt(keylen32, off)
	h.f.ReadAt(vallen32, off+4)
	// turn to ints
	k_len := binary.BigEndian.Uint32(keylen32)

	// now read the value + return
	res := make([]byte, int(k_len))
	h.f.ReadAt(res, off+8+int64(k_len))
	return res, nil
}
