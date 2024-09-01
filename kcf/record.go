package kcf

import "encoding/binary"
import "hash/crc32"
import "errors"

type RecordType uint8

const (
	MARKER         RecordType = 0x21
	ARCHIVE_HEADER RecordType = 0x41
	FILE_HEADER    RecordType = 0x46
	DATA_FRAGMENT  RecordType = 0x44
)

type RecordFlags uint8

const (
	HAS_ADDED_4     RecordFlags = 0b1000_0000
	HAS_ADDED_8     RecordFlags = 0b1100_0000
	HAS_ADDED_CRC32 RecordFlags = 0b0010_0000
)

type Record struct {
	HeadCRC   uint16
	HeadType  RecordType
	HeadFlags RecordFlags
	HeadSize  uint16

	AddedDataSize  uint64
	AddedDataCRC32 uint32

	Data []byte
}

func (rec Record) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0)
	data = binary.LittleEndian.AppendUint16(data, rec.HeadCRC)
	data = append(data, uint8(rec.HeadType))
	data = append(data, uint8(rec.HeadFlags))
	data = binary.LittleEndian.AppendUint16(data, rec.HeadSize)

	if rec.HeadFlags&HAS_ADDED_8 == HAS_ADDED_4 {
		data = binary.LittleEndian.AppendUint32(data,
			uint32(rec.AddedDataSize))
	} else if rec.HeadFlags&HAS_ADDED_8 == HAS_ADDED_8 {
		data = binary.LittleEndian.AppendUint64(data, rec.AddedDataSize)
	}

	if rec.HeadFlags&HAS_ADDED_CRC32 != 0 {
		data = binary.LittleEndian.AppendUint32(data, rec.AddedDataCRC32)
	}

	data = append(data, rec.Data...)

	err = nil
	return
}

func (rec Record) getHeadCRC() (crc uint16) {
	crc32c_table := crc32.MakeTable(crc32.Castagnoli)
	crc32c := crc32.New(crc32c_table)

	crc = 0

	var buffer []byte
	buffer, _ = rec.MarshalBinary()

	crc32c.Reset()
	crc32c.Write(buffer[2:]) // cut HeadCRC field
	crc = uint16(crc32c.Sum32() & 0xFFFF)

	return
}

func (rec Record) VaildateCRC() (isValid bool) {
	crc := rec.getHeadCRC()
	if crc != rec.HeadCRC {
		isValid = false
	} else {
		isValid = true
	}

	return
}

func (rec *Record) Fix() (err error) {
	headSize := 6

	if (rec.HeadFlags & HAS_ADDED_8) == HAS_ADDED_4 {
		headSize += 4
	} else if (rec.HeadFlags & HAS_ADDED_8) == HAS_ADDED_8 {
		headSize += 8
	}

	if (rec.HeadFlags & HAS_ADDED_CRC32) != 0 {
		headSize += 4
	}

	recSize := int(headSize) + len(rec.Data)
	if recSize > 65535 {
		err = errors.New("too big record data size")
		return
	}
	rec.HeadCRC = rec.getHeadCRC()

	return
}

type RecordData interface {
	AsRecord() (Record, error)
}

type ArchiveHeader struct {
	Version uint16
}

func (ahdr ArchiveHeader) AsRecord() (record Record, err error) {
	record.HeadType = ARCHIVE_HEADER
	record.HeadFlags = 0

	record.Data = binary.LittleEndian.AppendUint16(nil, ahdr.Version)
	err = record.Fix()

	return
}

type FileFlags uint8
type FileType uint8

const (
	REGULAR_FILE FileType = 0x46
	DIRECTORY    FileType = 0x64
)

const (
	HAS_TIMESTAMP  FileFlags = 0b0000_0001
	HAS_FILE_CRC32 FileFlags = 0b0000_0010
	HAS_UNPACKED_4 FileFlags = 0b0000_0100
	HAS_UNPACKED_8 FileFlags = 0b0000_1100
)

type FileHeader struct {
	FileFlags       FileFlags
	FileType        FileType
	UnpackedSize    uint64
	FileCRC32       uint32
	CompressionInfo uint32
	TimeStamp       uint64
	FileName        string
}

func (fhdr FileHeader) AsRecord() (rec Record, err error) {
	if len(fhdr.FileName) > 65535 {
		err = errors.New("too long file name: more than 65535 bytes")
		return
	}

	data := make([]byte, 0)
	rec.HeadType = FILE_HEADER

	data = append(data, uint8(fhdr.FileFlags))
	data = append(data, uint8(fhdr.FileType))

	if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_4 {
		data = binary.LittleEndian.AppendUint32(data,
			uint32(fhdr.UnpackedSize))
	} else if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_8 {
		data = binary.LittleEndian.AppendUint64(data, fhdr.UnpackedSize)
	}

	if (fhdr.FileFlags & HAS_FILE_CRC32) != 0 {
		data = binary.LittleEndian.AppendUint32(data, fhdr.FileCRC32)
	}

	data = binary.LittleEndian.AppendUint32(data, fhdr.CompressionInfo)

	if (fhdr.FileFlags & HAS_TIMESTAMP) != 0 {
		data = binary.LittleEndian.AppendUint64(data, fhdr.TimeStamp)
	}

	fileNameBytes := []byte(fhdr.FileName)
	fileNameSize := len(fileNameBytes)
	data = binary.LittleEndian.AppendUint16(data, uint16(fileNameSize))
	data = append(data, fileNameBytes...)

	return
}
