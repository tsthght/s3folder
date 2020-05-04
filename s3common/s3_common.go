package s3common

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

type MafkaClientVersion struct {
	Version [3]uint
}

func newMafkaClientVersion(major, minor, veryMinor uint) MafkaClientVersion {
	return MafkaClientVersion{
		Version: [3]uint{major, minor, veryMinor},
	}
}

func (v MafkaClientVersion) String() string {
	return fmt.Sprintf("%d.%d.%d(go)", v.Version[0], v.Version[1], v.Version[2])
}

var (
	V0_1_1                     = newMafkaClientVersion(0, 1, 1)
	ConnectionRetryTimes int   = 256
	GTimeStamp           int64 = -1
	GTimeStampMS         int64 = -1
)

func GetCurrentTime() int64 {
	return time.Now().UnixNano() / 1000
}

func GetIncTimeStamp() int64 {
	var swapped bool
	var newTS int64
	var oldTS int64
	for swapped != true {
		newTS = GetCurrentTime()
		oldTS = atomic.LoadInt64(&GTimeStamp)
		if newTS <= oldTS {
			newTS = oldTS + 1
		}
		swapped = atomic.CompareAndSwapInt64(&GTimeStamp, oldTS, newTS)
	}
	return newTS
}

func GetIncTimeStampMS() int64 {
	var swapped bool
	var newTS int64
	var oldTS int64
	for swapped != true {
		newTS = GetCurrentTime() / 1000
		oldTS = atomic.LoadInt64(&GTimeStampMS)
		if newTS <= oldTS {
			newTS = oldTS + 1
		}
		swapped = atomic.CompareAndSwapInt64(&GTimeStampMS, oldTS, newTS)
	}
	return newTS
}

func GetFloatFormatTimeStampS(timeStampMS int64) string {
	return strconv.FormatFloat(float64(timeStampMS)/1000000, 'f', -1, 64)
}
