package kcf

import "hash/crc32"
import "io"
import "errors"

func (kcf *Kcf) writeRecord(rec Record) (n int64, err error) {
	if !kcf.state.IsWriting() {
		panic(InvalidState)
	}

	if kcf.state.GetStage() == stageRecordAddedData {
		err = kcf.finishAddedData()
		if err != nil {
			return
		}
	}

	if kcf.state.GetStage() != stageRecordHeader {
		panic(InvalidState)
	}

	kcf.recOffset, err = kcf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	n, err = rec.WriteTo(kcf.file)
	if rec.HeadFlags&HAS_ADDED_4 != 0 {
		kcf.state.SetStage(stageRecordAddedData)
		kcf.lastRecord = rec

		kcf.addedWriter.W = kcf.file
		if rec.AddedDataSize > 0 {
			kcf.state.SetAddedSizeKnown(true)
			kcf.available = rec.AddedDataSize
			kcf.addedWriter.N = int64(rec.AddedDataSize)
		}
		if rec.HasAddedCRC32() {
			kcf.state.SetHasAddedCRC(true)
			if kcf.crc32 != nil {
				kcf.crc32.Reset()
			} else {
				crc32c_table := crc32.MakeTable(crc32.Castagnoli)
				kcf.crc32 = crc32.New(crc32c_table)
			}
		}
	}

	return
}

type LimitedWriter struct {
	W io.Writer
	N int64
}

var LimitedWrite = errors.New("write has been limited")

func (lw *LimitedWriter) Write(buf []byte) (n int, err error) {
	if lw.N <= 0 {
		return 0, LimitedWrite
	}

	to_read := int64(len(buf))
	if to_read > lw.N {
		to_read = lw.N
	}

	n, err = lw.W.Write(buf[:to_read])
	lw.N -= int64(n)

	return
}

func LimitWriter(w io.Writer, n int64) (lw *LimitedWriter) {
	lw = new(LimitedWriter)
	lw.W = w
	lw.N = n

	return
}

func (kcf *Kcf) writeAddedData(buf []byte) (n int, err error) {
	if !kcf.state.IsWriting() {
		panic(InvalidState)
	}

	if kcf.state.GetStage() != stageRecordAddedData {
		panic(InvalidState)
	}

	if kcf.state.IsAddedSizeKnown() {
		n, err = kcf.addedWriter.Write(buf)
	} else {
		n, err = kcf.file.Write(buf)
	}

	kcf.available -= uint64(n)
	kcf.written += uint64(n)
	if kcf.state.HasAddedCRC() {
		kcf.crc32.Write(buf[:n])
	}

	return
}

func (kcf *Kcf) finishAddedData() (err error) {
	if !kcf.state.IsWriting() {
		panic(InvalidState)
	}

	if kcf.state.GetStage() != stageRecordAddedData {
		panic(InvalidState)
	}

	if !kcf.state.HasAddedCRC() && kcf.state.IsAddedSizeKnown() {
		kcf.state.SetStage(stageRecordHeader)
		kcf.state.SetAddedSizeKnown(false)
		return
	}

	if kcf.state.IsAddedCRCKnown() && kcf.state.IsAddedSizeKnown() {
		kcf.state.SetStage(stageRecordHeader)
		kcf.state.SetAddedSizeKnown(false)
		kcf.state.SetAddedCRCKnown(false)
		kcf.state.SetHasAddedCRC(false)
		return
	}

	kcf.recEndOffset, err = kcf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	_, _ = kcf.file.Seek(kcf.recOffset, io.SeekStart)
	kcf.lastRecord.AddedDataCRC32 = kcf.crc32.Sum32()
	kcf.lastRecord.AddedDataSize = kcf.written
	kcf.lastRecord.Fix()
	_, err = kcf.lastRecord.WriteTo(kcf.file)
	if err != nil {
		return
	}
	_, _ = kcf.file.Seek(kcf.recEndOffset, io.SeekStart)

	kcf.state.SetStage(stageRecordHeader)

	return
}

func (kcf *Kcf) writeMarker() (err error) {
	if !kcf.state.IsWriting() {
		panic(InvalidState)
	}

	if kcf.state.GetStage() == stageNothing {
		kcf.state.SetStage(stageMarker)
	}

	if kcf.state.GetStage() != stageMarker {
		panic(InvalidState)
	}

	marker := []byte{0, 0, 0, 0, 0, 0}
	marker[0] = 0x4B
	marker[1] = 0x43
	marker[2] = 0x21
	marker[3] = 0x1A
	marker[4] = 0x06
	marker[5] = 0x00

	_, err = kcf.file.Write(marker)
	if err != nil {
		return
	}

	kcf.state.SetStage(stageRecordHeader)

	return
}
