package kcf

import "errors"

var InvalidFormat = errors.New("kcf: invalid archive format")
var CorruptedRecordData = errors.New("kcf: invalid record data")
var TooBigFileName = errors.New("record: too big file name, " +
	"more than 65535 bytes")
var TooBigRecordData = errors.New("record: too big record data")

var InvalidState = errors.New("kcf: invalid state")
var InvalidAddedData = errors.New("kcf: invalid added data")
