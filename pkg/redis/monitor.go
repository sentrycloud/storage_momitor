package redis

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/sentrycloud/sentry-sdk-go"
	"github.com/sentrycloud/sentry/pkg/newlog"
	"github.com/sentrycloud/storage_momitor/pkg/config"
	"github.com/sentrycloud/storage_momitor/pkg/util"
	"strconv"
	"strings"
	"time"
)

func StartMonitor() {
	for _, redisConfig := range config.ServerConfig.Redis {
		go monitor(redisConfig)
	}
}

func monitor(redisConfig config.RedisConfig) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisConfig.ServerAddr,
		Password: redisConfig.Password,
		DB:       0,
	})

	tags := map[string]string{}
	tags["server"] = redisConfig.ServerAddr

	qpsCollector := sentry.GetCollector("redis_qps", tags, sentry.Sum, redisConfig.CollectInterval)
	inFlowCollector := sentry.GetCollector("redis_in_flow", tags, sentry.Sum, redisConfig.CollectInterval)
	outFlowCollector := sentry.GetCollector("redis_out_flow", tags, sentry.Sum, redisConfig.CollectInterval)
	memCollector := sentry.GetCollector("redis_mem", tags, sentry.Sum, redisConfig.CollectInterval)
	memPercentCollector := sentry.GetCollector("redis_mem_percent", tags, sentry.Sum, redisConfig.CollectInterval)
	clientConnCollector := sentry.GetCollector("redis_conn", tags, sentry.Sum, redisConfig.CollectInterval)
	keysCollector := sentry.GetCollector("redis_keys", tags, sentry.Sum, redisConfig.CollectInterval)
	hitRateCollector := sentry.GetCollector("redis_hit_rate", tags, sentry.Sum, redisConfig.CollectInterval)

	for {
		startTime := time.Now()
		infoCmd := rdb.InfoMap(context.Background(), "Stats", "Memory", "Clients", "Keyspace")

		if infoCmd.Err() != nil {
			newlog.Error("redis info cmd failed: %v", infoCmd.Err())
		} else {
			infoMap := infoCmd.Val()
			startTimestamp := startTime.Unix()

			collectSimpleData(infoMap, "Stats", "instantaneous_ops_per_sec", qpsCollector, startTimestamp)
			collectSimpleData(infoMap, "Stats", "instantaneous_input_kbps", inFlowCollector, startTimestamp)
			collectSimpleData(infoMap, "Stats", "instantaneous_output_kbps", outFlowCollector, startTimestamp)
			collectSimpleData(infoMap, "Memory", "used_memory", memCollector, startTimestamp)
			collectSimpleData(infoMap, "Clients", "connected_clients", clientConnCollector, startTimestamp)

			collectMemPercent(infoMap, memPercentCollector, startTimestamp)
			collectHitRate(infoMap, hitRateCollector, startTimestamp)
			collectKeys(infoMap, keysCollector, startTimestamp)
		}

		util.SleepInterval(redisConfig.CollectInterval, startTime)
	}
}

func collectSimpleData(infoMap map[string]map[string]string, section string, key string, collector sentry.Collector, now int64) {
	if infos, exist := infoMap[section]; exist {
		if valStr, ok := infos[key]; ok {
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				newlog.Error("parseFloat failed: valStr=%s, err=%v", valStr, err)
			} else {
				collector.PutWithTime(val, now)
			}
		}
	}
}

func collectMemPercent(infoMap map[string]map[string]string, collector sentry.Collector, now int64) {
	usedMem, err := getStatsValue(infoMap, "Memory", "used_memory")
	maxMem, err2 := getStatsValue(infoMap, "Memory", "maxmemory")
	if err == nil && err2 == nil {
		memPercent := 0.0
		if maxMem > 0 {
			memPercent = usedMem / maxMem * 100
		}

		collector.PutWithTime(memPercent, now)
	}
}

func collectHitRate(infoMap map[string]map[string]string, collector sentry.Collector, now int64) {
	hit, err := getStatsValue(infoMap, "Stats", "keyspace_hits")
	miss, err2 := getStatsValue(infoMap, "Stats", "keyspace_misses")
	if err == nil && err2 == nil {
		hitRate := 0.0
		if hit+miss > 0 {
			hitRate = hit / (hit + miss) * 100
		}

		collector.PutWithTime(hitRate, now)
	}
}

func collectKeys(infoMap map[string]map[string]string, collector sentry.Collector, now int64) {
	if infos, exist := infoMap["Keyspace"]; exist {
		keyCount := 0.0
		for _, info := range infos {
			infoArr := strings.Split(info, ",")
			keysArr := strings.Split(infoArr[0], "=")
			if len(keysArr) == 2 {
				keys, err := strconv.ParseFloat(keysArr[1], 64)
				if err == nil {
					keyCount += keys
				}
			}
		}

		collector.PutWithTime(keyCount, now)
	}
}

func getStatsValue(infoMap map[string]map[string]string, section, key string) (float64, error) {
	if infos, exist := infoMap[section]; exist {
		if valStr, ok := infos[key]; ok {
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				newlog.Error("parseFloat for section=%s, key=%s failed: valStr=%s, err=%v", section, key, valStr, err)
			}

			return val, err
		}
	}

	return 0, errors.New("no such section or key")
}
