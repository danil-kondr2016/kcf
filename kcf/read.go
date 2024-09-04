package kcf

import (
	"hash/crc32"
	"io"
)

func (kcf *Kcf) readRecord() (rec Record, err error) {
	if !kcf.state.IsReading() {
		err = InvalidState
		return
	}

	if kcf.state.GetStage() != stageRecordHeader {
		err = InvalidState
		return
	}

	_, err = kcf.lastRecord.ReadFrom(kcf.file)
	if err != nil {
		return
	}

	rec = kcf.lastRecord
	if rec.HasAddedSize() {
		kcf.addedReader.R = kcf.file
		kcf.addedReader.N = int64(rec.AddedDataSize)

		if rec.HasAddedCRC32() {
			kcf.validCrc = rec.AddedDataCRC32
			if kcf.crc32 == nil {
				crc32c_table := crc32.MakeTable(crc32.Castagnoli)
				kcf.crc32 = crc32.New(crc32c_table)
			} else {
				kcf.crc32.Reset()
			}
		}
		kcf.state.SetStage(stageRecordAddedData)
	}
	return
}

func (kcf *Kcf) skipRecord() (err error) {
	if !kcf.state.IsReading() {
		err = InvalidState
		return
	}

	if kcf.state.GetStage() != stageRecordHeader {
		err = InvalidState
		return
	}

	_, err = kcf.readRecord()
	if err != nil {
		return
	}

	if kcf.lastRecord.HasAddedSize() {
		_, err = io.CopyN(io.Discard, kcf.file,
			int64(kcf.lastRecord.AddedDataSize))
	}

	kcf.state.SetStage(stageRecordHeader)

	return
}

func (kcf *Kcf) skipAddedData() (err error) {
	if !kcf.state.IsReading() {
		return InvalidState
	}

	if kcf.state.GetStage() != stageRecordAddedData {
		return InvalidState
	}

	_, err = io.CopyN(io.Discard, kcf.file, kcf.addedReader.N)
	kcf.state.SetStage(stageRecordHeader)

	return
}

func (kcf *Kcf) readAddedData(buf []byte) (n int, err error) {
	if !kcf.state.IsReading() {
		return 0, InvalidState
	}

	if kcf.state.GetStage() != stageRecordAddedData {
		return 0, InvalidState
	}

	if kcf.available == 0 {
		return 0, io.EOF
	}

	lr := io.LimitReader(kcf.file, int64(kcf.available))
	n, err = lr.Read(buf)
	if err == nil || err == io.EOF {
		kcf.available -= uint64(n)
	}

	if kcf.state.HasAddedCRC() {
		kcf.crc32.Write(buf[:n])
		if kcf.available == 0 && kcf.crc32.Sum32() != kcf.validCrc {
			err = InvalidAddedData
		}
	}

	return
}

func (kcf *Kcf) scanForMarker() (err error) {
	var marker [6]byte

	if !kcf.state.IsReading() {
		err = InvalidState
		return
	}

	if kcf.state.GetStage() == stageNothing {
		kcf.state.SetStage(stageMarker)
	}

	if kcf.state.GetStage() != stageMarker {
		err = InvalidState
		return
	}

	for {
		marker[0] = marker[1]
		marker[1] = marker[2]
		marker[2] = marker[3]
		marker[3] = marker[4]
		marker[4] = marker[5]

		_, err = kcf.file.Read(marker[5:6])

		if err != nil {
			err = InvalidFormat
			return
		}

		if marker[0] == 0x4B &&
			marker[1] == 0x43 &&
			marker[2] == 0x21 &&
			marker[3] == 0x1A &&
			marker[4] == 0x06 &&
			marker[5] == 0x00 {
			break
		}
	}

	kcf.state.SetStage(stageRecordHeader)

	return
}
