package hash_indexes

import (
	"encoding/binary"
	"errors"
	"fmt"
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

/*
V2 Design:
Fill Segment File before moving onto the next one
Preparation for Compaction
*/
type SegmentFile struct {
	f    *os.File
	size int64
}

type Entry struct {
	f           *os.File
	byte_offset int64
}
type HashIndex struct {
	f        *os.File         // TODO get rid of this LOL
	segments []*SegmentFile   // Array of Current Segment Files
	index    map[string]Entry // key -> offset in log
	maxSize  int64            // maximum size of a segment file
}

func NewHashIndex() (*HashIndex, error) {
	file, err := os.OpenFile("dataseg_0", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	seg := []*SegmentFile{{f: file, size: 0}}
	return &HashIndex{
		f:        file,
		segments: seg,
		index:    make(map[string]Entry),
		maxSize:  32 * 1024, // for now
	}, nil
}

func (h *HashIndex) Put(key, value []byte) error {
	current_segment := h.segments[len(h.segments)-1]
	file := current_segment.f
	lastByte, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// KEY_LEN, VAL_LEN
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)

	total_entry_size := 8 + int64(len(key)) + int64(len(value))
	// Special Case: If we are going to be writing what will exceed HashIndex Limit make new segment
	if current_segment.size+total_entry_size > h.maxSize {
		h.createDataSegment()
		current_segment = h.segments[len(h.segments)-1]
		file = current_segment.f
		lastByte = 0
	}

	log.Printf("Key Length is Currently %d\n", uint32(len(key)))
	log.Printf("Value Length is Currently %d\n", uint32(len(value)))
	// place using big endian
	binary.BigEndian.PutUint32(keylen32, uint32(len(key)))
	binary.BigEndian.PutUint32(vallen32, uint32(len(value)))
	// write these to dataseg
	h.index[string(key)] = Entry{f: file, byte_offset: lastByte}
	log.Printf("lastByte currently is %d\n", lastByte)
	file.WriteAt(keylen32, lastByte)
	file.WriteAt(vallen32, lastByte+4)
	// now write KV pair
	file.WriteAt(key, lastByte+8)
	file.WriteAt(value, lastByte+int64(8)+int64(len(key)))
	file.Sync()
	return nil
}

func (h *HashIndex) Get(key []byte) ([]byte, error) {
	// TODO: Fetch from map, read from byte offset
	entry, ok := h.index[string(key)]
	off := entry.byte_offset
	file := entry.f
	if !ok {
		return []byte{}, errors.New("key does not exist")
	}
	// Seek, then read
	// Skip Key, Read value len, go to position lastByte + 8 + key_len, read value & return
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)

	file.ReadAt(keylen32, off)
	file.ReadAt(vallen32, off+4)
	// turn to ints
	k_len := binary.BigEndian.Uint32(keylen32)
	v_len := binary.BigEndian.Uint32(vallen32)
	log.Printf("k_len=%d v_len=%d\n", k_len, v_len)

	value := make([]byte, v_len)
	file.ReadAt(value, off+8+int64(k_len))
	return value, nil

}

func (h *HashIndex) Close() error {
	return h.f.Close()
}

func (h *HashIndex) createDataSegment() error {
	index := len(h.segments)
	file, err := os.OpenFile(fmt.Sprintf("dataseg_%d", index), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	h.segments = append(h.segments, &SegmentFile{f: file, size: 0})
	return nil
}
