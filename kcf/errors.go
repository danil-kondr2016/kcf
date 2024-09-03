package kcf

import "errors"

var InvalidFormat = errors.New("kcf: invalid archive format")
var TooBigFileName = errors.New("record: too big file name, " +
	"more than 65535 bytes")
var TooBigRecordData = errors.New("record: too big record data")
