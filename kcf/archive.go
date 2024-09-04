package kcf

import "os"
import "io"
import "hash"

// bit 0, 1 - parser mode
// b10
//
//	00 - nothing
//	01 - reading
//	10 - writing
//	11 - invalid
//
// bit 2, 3, 4, 5 - parser stage
// b5432
//
//	0000 - nothing
//	0001 - marker
//	0002 - record header
//	0003 - record main data
//	0004 - record added data
//
// bit 6 - should validate CRC32 of added data
type kcfState uint64

type mode uint64

const (
	modeNothing mode = iota
	modeRead
	modeWrite
	modeMask
)

type stage uint64

const (
	stageNothing stage = (iota << 2)
	stageMarker
	stageRecordHeader
	stageRecordData
	stageRecordAddedData
	stageMask stage = 0b111100
)

const (
	flagAddedCRC kcfState = (1 << (iota + 6))
)

func (state kcfState) IsReading() bool {
	return mode(uint64(state)&uint64(modeMask)) == modeRead
}

func (state kcfState) IsWriting() bool {
	return mode(uint64(state)&uint64(modeMask)) == modeWrite
}

func (state *kcfState) SetMode(m mode) {
	x := uint64(*state)
	x &^= uint64(modeMask)
	x |= uint64(m & modeMask)
	*state = kcfState(x)
}

func (state kcfState) GetStage() stage {
	return stage(uint64(state) & uint64(stageMask))
}

func (state *kcfState) SetStage(s stage) {
	x := uint64(*state)
	x &^= uint64(stageMask)
	x |= uint64(s & stageMask)
	*state = kcfState(x)
}

func (state kcfState) ShouldValidateAddedCRC() bool {
	return state&flagAddedCRC != 0
}

func (state *kcfState) SetShouldValidateAddedCRC(x bool) {
	*state &^= flagAddedCRC
	if x {
		*state |= flagAddedCRC
	}
}

type Kcf struct {
	state        kcfState
	available    uint64
	recOffset    int64
	recEndOffset int64
	validCrc     uint32

	isWritable bool
	isSeekable bool

	crc32       hash.Hash32
	file        *os.File
	addedReader io.LimitedReader

	lastRecord  Record
	currentFile FileHeader
}

func (kcf Kcf) IsWritable() bool {
	return kcf.isWritable
}

func (kcf Kcf) IsSeekable() bool {
	return kcf.isSeekable
}

func CreateNewArchive(path string) (kcf *Kcf, err error) {
	kcf = new(Kcf)
	kcf.file, err = os.Create(path)
	if err != nil {
		kcf = nil
		return
	}

	kcf.state.SetMode(modeWrite)
	kcf.isWritable = true

	var err1 error
	_, err1 = kcf.file.Seek(0, io.SeekCurrent)
	if err1 == nil {
		kcf.isSeekable = true
	}

	return
}

func OpenArchive(path string) (kcf *Kcf, err error) {
	kcf = new(Kcf)
	kcf.file, err = os.Open(path)
	if err != nil {
		kcf = nil
		return
	}

	kcf.state.SetMode(modeRead)

	var err1 error
	_, err1 = kcf.file.Seek(0, io.SeekCurrent)
	if err1 == nil {
		kcf.isSeekable = true
	}

	return
}

func (kcf *Kcf) Close() (err error) {
	kcf.addedReader.R = nil
	kcf.addedReader.N = 0
	kcf.crc32 = nil
	err = kcf.file.Close()

	return
}
