package s3common

import (
	"sync"
	"testing"
)

type timeCounter struct {
	loopCnt int
	ret     []int64
	wg      *sync.WaitGroup
}

func NewTimeCounter(loopCnt int, wg *sync.WaitGroup) *timeCounter {
	return &timeCounter{
		loopCnt: loopCnt,
		wg:      wg,
	}
}

func (tc *timeCounter) run() {
	for i := 0; i < tc.loopCnt; i++ {
		ts := GetIncTimeStamp()
		tc.ret = append(tc.ret, ts)
	}
	tc.wg.Done()
}

func checkIncrement(s []int64) bool {
	var ret bool
	size := len(s)
	if size < 2 {
		ret = true
	} else {
		ret = true
		for i := 0; i < size-1; i++ {
			if s[i] > s[i+1] {
				ret = false
				break
			}
		}
	}
	return ret
}

var (
	NumOfTimeCounter int = 200
	LoopCnt          int = 10000
)

func TestTimeStamp(t *testing.T) {
	var wg sync.WaitGroup
	var counters []*timeCounter
	for i := 0; i < NumOfTimeCounter; i++ {
		tc := NewTimeCounter(LoopCnt, &wg)
		counters = append(counters, tc)
		wg.Add(1)
	}
	for _, c := range counters {
		c.run()
	}
	wg.Wait()

	for _, c := range counters {
		if !checkIncrement(c.ret) {
			t.Error("test increment failed")
		}
	}

	tsMap := make(map[int64]bool)
	for _, c := range counters {
		for _, ts := range c.ret {
			if _, ok := tsMap[ts]; ok {
				t.Errorf("ts corrupt, ts=%d", ts)
			} else {
				tsMap[ts] = true
			}
		}
	}
}
