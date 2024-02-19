package util

import "time"

// SleepInterval sleep interval - costTime,  costTime = nowTime - startTime, and must be much less than interval
func SleepInterval(interval int64, startTime time.Time) {
	costTime := time.Now().Sub(startTime)
	sleepTime := time.Duration(interval)*time.Second - costTime
	if sleepTime > 0 {
		time.Sleep(sleepTime)
	}
}
