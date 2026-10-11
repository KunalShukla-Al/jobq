// Package logstore is jobq's second storage backend: an append-only log on
// local disk that I own end to end, instead of Postgres. DESIGN-v2.md is the
// format and the rules; this package follows it.
//
// Every change to a job is a record appended to the newest segment file.
// The current state of every job lives in memory and is rebuilt on start by
// replaying the records. A request is answered only after its records are
// fsynced, so a crash can lose work nobody was told about, never work that
// was acknowledged.
package logstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// recType is a record's type byte.
type recType uint8

const (
	recEnqueue recType = iota + 1
	recClaim
	recComplete
	recRetry
	recKill
	recDelete
	// A checkpoint replaces everything before it: begin (with the next id),
	// one put per job, end. Compaction writes one into a fresh segment.
	recCheckpointBegin
	recPut
	recCheckpointEnd
)

func (t recType) String() string {
	names := [...]string{"?", "enqueue", "claim", "complete", "retry", "kill", "delete",
		"checkpoint-begin", "put", "checkpoint-end"}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("type-%d", t)
}

const (
	// headerSize is length (4) + CRC-32C (4) + type (1).
	headerSize = 9
	// maxRecord bounds a record's length field, so a corrupt length can't make
	// recovery allocate gigabytes. Payloads are small JSON documents.
	maxRecord = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errTorn means the bytes at this position aren't a whole, intact record: a
// write cut short by a crash, or corruption. Recovery stops there.
var errTorn = errors.New("torn or corrupt record")

// appendRecord encodes one record onto buf:
//
//	length uint32 | CRC-32C uint32 | type uint8 | body
//
// big-endian; length counts type + body, and the CRC covers them too.
func appendRecord(buf []byte, t recType, body []byte) []byte {
	var h [headerSize]byte
	binary.BigEndian.PutUint32(h[0:4], uint32(1+len(body)))
	binary.BigEndian.PutUint32(h[4:8], checksum(t, body))
	h[8] = byte(t)
	return append(append(buf, h[:]...), body...)
}

func checksum(t recType, body []byte) uint32 {
	return crc32.Update(crc32.Update(0, castagnoli, []byte{byte(t)}), castagnoli, body)
}

// readRecord reads the next record. io.EOF means a clean end exactly at a
// record boundary; errTorn means a partial or corrupt record starts here.
// size is the bytes the record took on disk.
func readRecord(r *bufio.Reader) (t recType, body []byte, size int, err error) {
	var h [headerSize]byte
	n, err := io.ReadFull(r, h[:])
	if err == io.EOF {
		return 0, nil, 0, io.EOF
	}
	if err != nil {
		return 0, nil, n, errTorn // a header cut short
	}
	length := binary.BigEndian.Uint32(h[0:4])
	if length < 1 || length > maxRecord {
		return 0, nil, 0, errTorn
	}
	body = make([]byte, length-1)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, 0, errTorn // a body cut short
	}
	t = recType(h[8])
	if checksum(t, body) != binary.BigEndian.Uint32(h[4:8]) || t < recEnqueue || t > recCheckpointEnd {
		return 0, nil, 0, errTorn
	}
	return t, body, headerSize + len(body), nil
}
