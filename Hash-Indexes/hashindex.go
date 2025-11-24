package main

import (
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
	// TODO: Simply Append to Logfile
	lastByte, err := h.f.Seek(-1, 2)
	if err != nil {
		return err
	}
	h.index[string(key)] = lastByte
	h.f.WriteAt(value, lastByte)
	return nil
}

func (h *HashIndex) Get(key []byte) ([]byte, error) {
	// TODO: Fetch from map, read from byte offset
	off, ok := h.index[string(key)]
	if !ok {
		return []byte{}, errors.New("This Key Doesn't Exist")
	}
	// Seek, then read
}

func (h *HashIndex) Delete(key []byte) error {

	return nil
}
