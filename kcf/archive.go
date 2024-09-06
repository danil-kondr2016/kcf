package kcf

import (
	"hash"
	"hash/crc32"
	"io"
	"os"
)

// bit 0, 1 - parser mode
// b10
// .00 - nothing
// .01 - reading
// .10 - writing
// .11 - invalid
//
// bit 2, 3, 4, 5 - parser stage
// b5432
// .0000 - nothing
// .0001 - marker
// .0002 - record header
// .0003 - record main data
// .0004 - record added data
//
// bit 6 - should validate CRC32 of added data
// bit 7 - has known size of added data
//
// bit 32, 33, 34, 35 - packer position
// b 35 34 33 32
// .  0  0  0  0  - nothing
// .  0  0  0  1  - archive header
// .  0  0  1  0  - file header
// .  0  0  1  1  - file data
// .  0  1  0  0  - file metadata
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

type packerPos uint64

const (
	pposNothing packerPos = (iota << 32)
	pposArchiveStart
	pposFileHeader
	pposFileData
	pposFileMetadata
	pposMask packerPos = (0b1111 << 32)
)

func (state kcfState) GetPackerPos() packerPos {
	return packerPos(uint64(state) & uint64(pposMask))
}

func (state *kcfState) SetPackerPos(ppos packerPos) {
	x := uint64(*state)
	x &^= uint64(pposMask)
	x |= uint64(ppos & pposMask)
	*state = kcfState(x)
}

const (
	flagAddedCRC kcfState = (1 << (iota + 6))
	flagKnownSize
	flagKnownAddedCRC
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

func (state kcfState) HasAddedCRC() bool {
	return state&flagAddedCRC != 0
}

func (state *kcfState) SetHasAddedCRC(x bool) {
	*state &^= flagAddedCRC
	if x {
		*state |= flagAddedCRC
	}
}

func (state kcfState) IsAddedSizeKnown() bool {
	return state&flagKnownSize != 0
}

func (state *kcfState) SetAddedSizeKnown(x bool) {
	*state &^= flagKnownSize
	if x {
		*state |= flagKnownSize
	}
}

func (state kcfState) IsAddedCRCKnown() bool {
	return state&flagKnownAddedCRC != 0
}

func (state *kcfState) SetAddedCRCKnown(x bool) {
	*state &^= flagKnownAddedCRC
	if x {
		*state |= flagKnownAddedCRC
	}
}

type Kcf struct {
	state        kcfState
	available    uint64
	written      uint64
	recOffset    int64
	recEndOffset int64
	validCrc     uint32

	isWritable bool
	isSeekable bool

	crc32       hash.Hash32
	file        *os.File
	addedReader io.LimitedReader
	addedWriter LimitedWriter

	lastRecord  Record
	currentFile FileHeader
	archiveHdr  ArchiveHeader
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
	kcf.state.SetPackerPos(pposArchiveStart)

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
	kcf.addedWriter.W = nil
	kcf.addedWriter.N = 0

	kcf.crc32 = nil
	err = kcf.file.Close()

	return
}

func (kcf *Kcf) GetCurrentFile() (info FileHeader, err error) {
	if !kcf.state.IsReading() {
		err = InvalidState
		return
	}

	switch kcf.state.GetPackerPos() {
	case pposArchiveStart:
		err = InvalidState
		return
	case pposFileHeader:
		_, err = kcf.readRecord()
		if err != nil {
			return
		}

		kcf.currentFile, err = RecordToFileHeader(kcf.lastRecord)
		if err != nil {
			return
		}

		kcf.state.SetPackerPos(pposFileData)
	case pposFileData:
		fallthrough
	case pposFileMetadata:
		break
	}

	info = kcf.currentFile
	return
}

func (kcf *Kcf) UnpackFile(w io.Writer) (n int64, err error) {
	if !kcf.state.IsReading() {
		return 0, InvalidState
	}

	if kcf.state.GetPackerPos() == pposFileHeader {
		_, err = kcf.GetCurrentFile()
		if err != nil {
			return
		}
	}

	if kcf.state.GetPackerPos() != pposFileData {
		return 0, InvalidState
	}

	var flag bool = true

	for flag || ((kcf.lastRecord.HeadFlags & 0x01) != 0) {
		flag = false

		if kcf.state.GetStage() == stageRecordAddedData {
			buffer := make([]byte, 4096)
			var n_read, n_written int

			n_read, err = kcf.readAddedData(buffer)
			if err != nil && err != io.EOF {
				return
			}

			if err == io.EOF && kcf.lastRecord.HeadFlags&0x01 != 0 {
				_, err = kcf.readRecord()
				if err != nil {
					return
				}

				if kcf.lastRecord.HeadType != DATA_FRAGMENT {
					err = InvalidFormat
					return
				} else {
					continue
				}
			}

			// TODO compression if w != io.Discard
			// and compression algorithm has been specified
			n_written, err = w.Write(buffer[:n_read])
			if err != nil {
				return
			}

			n += int64(n_written)
		}
	}

	return
}

func (kcf *Kcf) PackFileRaw(file *os.File) (err error) {
	var info os.FileInfo

	info, err = file.Stat()
	if err != nil {
		return err
	}

	size := uint64(info.Size())

	var hdr FileHeader
	hdr.FileFlags = HAS_TIMESTAMP

	if info.IsDir() {
		hdr.FileType = DIRECTORY
	} else {
		hdr.FileType = REGULAR_FILE
		hdr.FileFlags |= HAS_UNPACKED_4
		if info.Size() > 2147483647 {
			hdr.FileFlags |= HAS_UNPACKED_8
		}

		hdr.UnpackedSize = size
	}

	kcf.currentFile = hdr
	kcf.lastRecord, err = kcf.currentFile.AsRecord()
	if err != nil {
		return err
	}

	kcf.lastRecord.HeadFlags |= HAS_ADDED_4
	if size > 2147483647 {
		kcf.lastRecord.HeadFlags |= HAS_ADDED_8
	}
	kcf.lastRecord.AddedDataSize = size

	if !kcf.isSeekable {
		kcf.lastRecord.HeadFlags |= 0x01
		kcf.lastRecord.AddedDataSize = 0
		kcf.lastRecord.HeadFlags &^= HAS_ADDED_8
	}

	kcf.writeRecord(kcf.lastRecord)

	var buffer []byte
	var n int
	if kcf.isSeekable {
		for {
			n, err = file.Read(buffer)

			if err != nil && err != io.EOF {
				return
			}

			if err == io.EOF {
				break
			}

			_, err = kcf.writeAddedData(buffer[:n])
			if err != nil {
				return
			}
		}

		kcf.finishAddedData()
	} else {
		crc32c_table := crc32.MakeTable(crc32.Castagnoli)
		crc32c := crc32.New(crc32c_table)

		for {
			n, err = file.Read(buffer)
			if err != nil && err != io.EOF {
				return
			}

			if err == io.EOF {
				break
			}

			crc32c.Reset()
			crc32c.Write(buffer[:n])
			kcf.state.SetAddedCRCKnown(true)
			kcf.lastRecord.HeadType = DATA_FRAGMENT
			kcf.lastRecord.AddedDataSize = uint64(n)
			kcf.lastRecord.AddedDataCRC32 = crc32c.Sum32()
			kcf.lastRecord.HeadFlags = HAS_ADDED_4 | HAS_ADDED_CRC32
			_, err = kcf.writeRecord(kcf.lastRecord)
			if err != nil {
				return
			}

			_, err = kcf.writeAddedData(buffer[:n])
			if err != nil {
				return
			}
			kcf.state.SetAddedCRCKnown(false)
		}
	}

	return
}

func (kcf *Kcf) InitArchive() (err error) {
	if kcf.state.GetPackerPos() != pposArchiveStart {
		err = InvalidState
		return
	}

	if kcf.state.IsWriting() {
		err = kcf.writeMarker()
		if err != nil {
			return
		}

		kcf.lastRecord, err = ArchiveHeader{Version: 1}.AsRecord()
		if err != nil {
			return
		}

		_, err = kcf.writeRecord(kcf.lastRecord)
		if err != nil {
			return
		}
	} else if kcf.state.IsReading() {
		err = kcf.scanForMarker()
		if err != nil {
			return
		}

		_, err = kcf.readRecord()
		if err != nil {
			return
		}

		kcf.archiveHdr, err = RecordToArchiveHeader(kcf.lastRecord)
		if err != nil {
			return
		}
	}

	kcf.state.SetPackerPos(pposFileHeader)

	return
}
