package hash_indexes

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
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

/*
V3 Design (Hard Part)
Implement Compaction + Merging
The Idea is that we want to perform this compaction/merging asynchronously
- P0: We must now make our HashMap thread safe. Eventually we will have to modify the byte offsets when we merge.
	  Therefore, if one thread is updating while another thread is merging, we need to avoid this race condition.
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
	log_mutex sync.Mutex // Single Writer to log files
	idx_mutex sync.RWMutex
	segments  []*SegmentFile   // Array of Current Segment Files
	index     map[string]Entry // key -> offset in log
	maxSize   int64            // maximum size of a segment file
}

func NewHashIndex() (*HashIndex, error) {
	file, err := os.OpenFile("dataseg_0", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	seg := []*SegmentFile{{f: file, size: 0}}
	return &HashIndex{
		segments: seg,
		index:    make(map[string]Entry),
		maxSize:  32 * 1024, // for now
	}, nil
}

func (h *HashIndex) Put(key, value []byte) error {

	total_entry_size := 8 + int64(len(key)) + int64(len(value))
	keylen32 := make([]byte, 4)
	vallen32 := make([]byte, 4)

	h.log_mutex.Lock()
	// KEY_LEN, VAL_LEN
	current_segment := h.segments[len(h.segments)-1]
	file := current_segment.f

	lastByte, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		h.log_mutex.Unlock()
		return err
	}
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

	log.Printf("lastByte currently is %d\n", lastByte)

	file.WriteAt(keylen32, lastByte)
	file.WriteAt(vallen32, lastByte+4)
	// now write KV pair
	file.WriteAt(key, lastByte+8)
	file.WriteAt(value, lastByte+int64(8)+int64(len(key)))
	file.Sync()
	h.log_mutex.Unlock()

	h.idx_mutex.Lock()
	h.index[string(key)] = Entry{f: file, byte_offset: lastByte}
	current_segment.size += total_entry_size
	h.idx_mutex.Unlock()
	return nil
}

func (h *HashIndex) Get(key []byte) ([]byte, error) {
	// TODO: Fetch from map, read from byte offset
	h.idx_mutex.Lock()
	entry, ok := h.index[string(key)]
	h.idx_mutex.Unlock()
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
	h.log_mutex.Lock()
	defer h.log_mutex.Unlock()

	var firstErr error
	for _, seg := range h.segments {
		if seg == nil || seg.f == nil {
			continue
		}
		if err := seg.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		seg.f = nil
	}
	h.segments = nil
	h.index = nil
	return firstErr
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
