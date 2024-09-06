package kcf

import "encoding/binary"
import "hash/crc32"
import "io"

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

var le = binary.LittleEndian

func (rec Record) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0)
	data = le.AppendUint16(data, rec.HeadCRC)
	data = append(data, uint8(rec.HeadType))
	data = append(data, uint8(rec.HeadFlags))
	data = le.AppendUint16(data, rec.HeadSize)

	if rec.HeadFlags&HAS_ADDED_8 == HAS_ADDED_4 {
		data = le.AppendUint32(data,
			uint32(rec.AddedDataSize))
	} else if rec.HeadFlags&HAS_ADDED_8 == HAS_ADDED_8 {
		data = le.AppendUint64(data, rec.AddedDataSize)
	}

	if rec.HeadFlags&HAS_ADDED_CRC32 != 0 {
		data = le.AppendUint32(data, rec.AddedDataCRC32)
	}

	data = append(data, rec.Data...)

	err = nil
	return
}

func (rec *Record) unmarshal(header []byte, data []byte) error {
	rec.HeadCRC = le.Uint16(header[0:])
	rec.HeadType = RecordType(header[2])
	rec.HeadFlags = RecordFlags(header[3])
	rec.HeadSize = le.Uint16(header[4:])

	ptr := 0
	if (rec.HeadFlags & HAS_ADDED_8) == HAS_ADDED_4 {
		rec.AddedDataSize = uint64(le.Uint32(data[ptr:]))
		ptr += 4
	} else if (rec.HeadFlags & HAS_ADDED_8) == HAS_ADDED_8 {
		rec.AddedDataSize = le.Uint64(data[ptr:])
		ptr += 8
	}

	if (rec.HeadFlags & HAS_ADDED_CRC32) != 0 {
		rec.AddedDataCRC32 = le.Uint32(data[ptr:])
		ptr += 4
	}

	rec.Data = make([]byte, int(rec.HeadSize)-ptr-len(header))
	copy(rec.Data, data[ptr:])

	return nil
}

func (rec *Record) UnmarshalBinary(data []byte) error {
	return rec.unmarshal(data[:6], data[6:])
}

func (rec *Record) ReadFrom(r io.Reader) (n int64, err error) {
	var Header, Data []byte
	var n_read int

	Header = make([]byte, 6)
	n_read, err = r.Read(Header)
	if err != nil {
		return 0, err
	}
	n += int64(n_read)

	to_read := int(le.Uint16(Header[4:])) - 6
	Data = make([]byte, to_read)
	n_read, err = r.Read(Data)
	if err != nil {
		return 0, err
	}
	n += int64(n_read)

	rec.unmarshal(Header, Data)
	if !rec.ValidateCRC() {
		err = CorruptedRecordData
	}
	return
}

func (rec Record) WriteTo(w io.Writer) (n int64, err error) {
	var n_read int = 0
	var buffer []byte

	buffer, err = rec.MarshalBinary()
	n_read, err = w.Write(buffer)
	if err != nil {
		return 0, err
	}

	n = int64(n_read)
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

func (rec Record) ValidateCRC() (isValid bool) {
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
		err = TooBigRecordData
		return
	}
	rec.HeadSize = uint16(recSize)
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

	record.Data = le.AppendUint16(nil, ahdr.Version)
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
		err = TooBigFileName
		return
	}

	data := make([]byte, 0)
	rec.HeadType = FILE_HEADER

	data = append(data, uint8(fhdr.FileFlags))
	data = append(data, uint8(fhdr.FileType))

	if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_4 {
		data = le.AppendUint32(data, uint32(fhdr.UnpackedSize))
	} else if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_8 {
		data = le.AppendUint64(data, fhdr.UnpackedSize)
	}

	if (fhdr.FileFlags & HAS_FILE_CRC32) != 0 {
		data = le.AppendUint32(data, fhdr.FileCRC32)
	}

	data = le.AppendUint32(data, fhdr.CompressionInfo)

	if (fhdr.FileFlags & HAS_TIMESTAMP) != 0 {
		data = le.AppendUint64(data, fhdr.TimeStamp)
	}

	fileNameBytes := []byte(fhdr.FileName)
	fileNameSize := len(fileNameBytes)
	data = le.AppendUint16(data, uint16(fileNameSize))
	data = append(data, fileNameBytes...)

	rec.Data = data
	rec.Fix()

	return
}

func RecordToArchiveHeader(rec Record) (
	ahdr ArchiveHeader,
	err error,
) {
	if !rec.ValidateCRC() {
		err = InvalidFormat
		return
	}

	if rec.HeadType != ARCHIVE_HEADER {
		err = InvalidFormat
		return
	}

	ahdr.Version = le.Uint16(rec.Data)
	return
}

func RecordToFileHeader(rec Record) (
	fhdr FileHeader,
	err error,
) {
	if !rec.ValidateCRC() {
		err = CorruptedRecordData
		return
	}

	if rec.HeadType != FILE_HEADER {
		err = InvalidFormat
		return
	}

	fhdr.FileFlags = FileFlags(rec.Data[0])
	fhdr.FileType = FileType(rec.Data[1])

	var ptr int = 2
	if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_4 {
		fhdr.UnpackedSize = uint64(le.Uint32(rec.Data[ptr:]))
		ptr += 4
	} else if (fhdr.FileFlags & HAS_UNPACKED_8) == HAS_UNPACKED_8 {
		fhdr.UnpackedSize = le.Uint64(rec.Data[ptr:])
		ptr += 8
	}

	if (fhdr.FileFlags & HAS_FILE_CRC32) != 0 {
		fhdr.FileCRC32 = le.Uint32(rec.Data[ptr:])
		ptr += 4
	}

	fhdr.CompressionInfo = le.Uint32(rec.Data[ptr:])
	ptr += 4

	if (fhdr.FileFlags & HAS_TIMESTAMP) != 0 {
		fhdr.TimeStamp = le.Uint64(rec.Data[ptr:])
		ptr += 8
	}

	fileNameSize := le.Uint16(rec.Data[ptr:])
	ptr += 2

	fileNameBytes := rec.Data[ptr : ptr+int(fileNameSize)]
	fhdr.FileName = string(fileNameBytes)

	return
}

func (rec Record) HasAddedSize() bool {
	return ((rec.HeadFlags & HAS_ADDED_4) != 0) &&
		rec.AddedDataSize > 0
}

func (rec Record) HasAddedCRC32() bool {
	return rec.HeadFlags&HAS_ADDED_CRC32 != 0
}
