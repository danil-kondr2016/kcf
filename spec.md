Kondratenko's Container File
============================

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY" and "OPTIONAL" in this 
document are to be interpreted as described in RFC 2119.

## Data layout

Archive file consists of variable length records. The order of these
records MAY vary, but the first record MUST be a marker record followed
by an archive header record.

All numeric values are represented as unsigned, little-endian values,
if not stated otherwise.

Each record begins with the following fields:

* `HeadCRC`,   2 bytes.  CRC of total record or record part
* `HeadType`,  1 byte.   Record type.
* `HeadFlags`, 1 byte.   Record flags.
* `HeadSize`,  2 bytes.  Record size.
* `AddedSize`, 4 or 8 bytes. Optional field - added data size.
* `AddedDataCRC32`, 4 bytes. Optional field - added data CRC32.

Field `AddedSize` present only if `(HeadFlags & 0x80) != 0` and has
size of 4 bytes. If `(HeadFlags & 0xC0) == 0xC0`, this field has
size of 8 bytes.

Total record size is `HeadSize` if `(HeadFlags & 0x80) == 0` and
`HeadSize+AddedSize` if the field `AddedSize` is present. `HeadSize`
includes header fields size and equals at least:

- 6  if `(HeadFlags & 0x80) == 0`;
- 10 if `(HeadFlags & 0xC0) == 0x80`;
- 14 if `(HeadFlags & 0xC0) == 0xC0`.

Field `AddedDataCRC32` present only if `(HeadFlags & 0x20) != 0` and has
size of 4 bytes. If this field is present, `HeadSize` equals at least:

- 10 if `(HeadFlags & 0x80) == 0 && (HeadFlags & 0x20) != 0`;
- 14 if `(HeadFlags & 0xC0) == 0x80 && (HeadFlags & 0x20) != 0`;
- 18 if `(HeadFlags & 0xC0) == 0xC0 && (HeadFlags & 0x20) != 0`.

## Record format

### Marker record

* `HeadCRC`,   2 bytes.  Always 0x434B

* `HeadType`,  1 byte.   Type:  0x21 (`!`)

* `HeadFlags`, 1 byte.   Always 0x1A

* `HeadSize`,  2 bytes.  Size = 0x0006

The marker record is actually considered as a fixed byte sequence:

    4B 43 21 1A 06 00

or in C-string notation:

    "KC!\x1A\6\0"

For this type of record any flags MUST be ignored since this record
begins KCF data stream. `HeadCRC` MUST NOT be validated.
    
### Archive header

* `HeadCRC`,   2 bytes. 

   CRC of fields from `HeadType` to `FormatVersion`

* `HeadType`,  1 byte.   Type:  0x41 (`A`)

* `HeadFlags`, 1 byte.   Always 0x00

* `HeadSize`,  2 bytes.  Size = 0x0008

* `FormatVersion`, 2 bytes. For this version of KCF it is fixed to 0x0001.

### File local header

* `HeadCRC`,   2 bytes.

   CRC of fields from `HeadType` to the end of `FileName`.

* `HeadType`,  1 byte.   Type:  0x46 (`F`)

* `HeadFlags`, 1 byte.  Bit flags:

  + 0x01: packed file data MUST be continued in the next record.

  + 0x80, 0x40, 0x20 are common bit flag values

* `HeadSize`,  2 bytes. 

  File header full size including file name.

* `PackedSize`, 4 or 8 bytes.

  Optional - packed data or data fragment size.

  If 0x01 head flag is set, total compressed data size is the sum 
  of `PackedSize` value of this record and `PackedSize` values of
  subsequent data fragment records.

* `PackedDataCRC32`, 4 bytes.

  Optional - packed data fragment CRC32. Usually it is not necessary.

* `FileFlags`, 1 byte. Bit flags:

  + 0x01: has POSIX-style timestamp (`TimeStamp` field)

  + 0x02: has `FileCRC32` field

  + 0x04: field `UnpackedSize` is present and 4 bytes long

  + 0x08: if 0x04 is set, `UnpackedSize` is 8 bytes long

* `FileType`, 1 byte. Type of file.

  + 0x46 (`'F'`) - regular file

  + 0x64 (`'d'`) - directory

* `UnpackedSize`, 4 or 8 bytes.

  Optional - uncompressed file size. Present if 0x04 flag is set.
  If 0x04 and 0x08 are set, this field is 8 bytes long.

  If this field is absent, the file is considered empty. Its packed
  data SHOULD be ignored and unpacker MUST create an empty file.

* `FileCRC32`, 4 bytes.

  Optional - CRC32 of the file. Calculated from uncompressed data.

* `CompressionInfo`, 4 bytes.

  The number specifies method of compression. Bits 0 to 7 are reserved
  for the compression method number, remaining bits values are dependent
  on the selected compression method. Bit 31 is reserved.

  If this field is set to zero, file has not been compressed.

* `TimeStamp`, 8 bytes, signed.

  Timestamp in POSIX format (count of seconds from January 1, 
  1970 00:00 UTC). Optional, present if 0x01 file flag is set.

* `FileNameSize`, 2 bytes.

  The length of full file name.

* `FileName`, `FileNameSize` bytes.

  File name encoded in UTF-8.

### Compressed data fragment record

This type of record MUST be placed after the file record or another 
data fragment record. Unpacker MUST ignore invalid data fragment 
record.

* `HeadCRC`,   2 bytes.

   CRC of fields from `HeadType` to `PackedDataCRC32` or `PackedSize`.

* `HeadType`,  1 byte.   Type:  0x44 (`D`)

* `HeadFlags`, 1 byte.  Bit flags:

  + 0x01: packed file data MUST be continued in the next record.

  + 0x80, 0x40, 0x20 are common bit flag values. 0x80 MUST be always
  set for this type of record.

* `HeadSize`,  2 bytes. 

  Valid values are 10, 14 or 18.

* `PackedSize`, 4 or 8 bytes.

  Optional - packed data fragment size.

  If 0x01 head flag is set, total compressed data size is the sum 
  of `PackedSize` value of this record and `PackedSize` values of
  subsequent data fragment records.

* `PackedDataCRC32`, 4 bytes.

  Optional - packed data fragment CRC32. Usually it is not necessary.

## Used CRC32

KCF uses CRC32C (Castagnoli CRC) algorithm which seems to be better than
traditional CRC32 used in zlib, gzip and other formats.

CRC16 is low-significant bytes of CRC32C.
