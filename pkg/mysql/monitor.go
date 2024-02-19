package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sentrycloud/sentry-sdk-go"
	"github.com/sentrycloud/sentry/pkg/newlog"
	"github.com/sentrycloud/storage_momitor/pkg/config"
	"github.com/sentrycloud/storage_momitor/pkg/util"
	"strconv"
	"time"
)

const (
	statusSQL = "show global status where Variable_name regexp 'Com_insert|Com_update|Com_delete|Com_select" +
		"|Questions|Slow_queries|Threads_connected|Max_used_connections'"
	setupSQL = "show global variables where Variable_name regexp 'max_connections|long_query_time'"
)

func StartMonitor() {
	for _, conf := range config.ServerConfig.MySQL {
		go monitor(&conf)
	}
}

func monitor(conf *config.MySQLConfig) {
	db := openMySQL(conf)
	if db == nil {
		return
	}

	tags := map[string]string{}
	tags["instance"] = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	insertQpsCollector := sentry.GetCollector("mysql_insert_qps", tags, sentry.Sum, conf.CollectInterval)
	updateQpsCollector := sentry.GetCollector("mysql_update_qps", tags, sentry.Sum, conf.CollectInterval)
	deleteQpsCollector := sentry.GetCollector("mysql_delete_qps", tags, sentry.Sum, conf.CollectInterval)
	selectQpsCollector := sentry.GetCollector("mysql_select_qps", tags, sentry.Sum, conf.CollectInterval)
	slowQpsCollector := sentry.GetCollector("mysql_slow_qps", tags, sentry.Sum, conf.CollectInterval)

	currentConnCollector := sentry.GetCollector("mysql_current_conn", tags, sentry.Sum, conf.CollectInterval)
	maxUsedConnCollector := sentry.GetCollector("mysql_max_used_conn", tags, sentry.Sum, conf.CollectInterval)
	maxConnCollector := sentry.GetCollector("mysql_max_conn", tags, sentry.Sum, conf.CollectInterval)
	connUsageCollector := sentry.GetCollector("mysql_conn_usage", tags, sentry.Sum, conf.CollectInterval)

	var prevStatusMap map[string]string
	for {
		startTime := time.Now()

		newStatusMap := queryMySQL(db, statusSQL)
		newSetupMap := queryMySQL(db, setupSQL)
		if newStatusMap != nil && newSetupMap != nil {
			if prevStatusMap != nil {
				currentTimeStamp := startTime.Unix()

				collectCounter(prevStatusMap, newStatusMap, "Com_insert", insertQpsCollector, currentTimeStamp)
				collectCounter(prevStatusMap, newStatusMap, "Com_update", updateQpsCollector, currentTimeStamp)
				collectCounter(prevStatusMap, newStatusMap, "Com_delete", deleteQpsCollector, currentTimeStamp)
				collectCounter(prevStatusMap, newStatusMap, "Com_select", selectQpsCollector, currentTimeStamp)
				collectCounter(prevStatusMap, newStatusMap, "Slow_queries", slowQpsCollector, currentTimeStamp)

				collectStatus(newStatusMap, "Threads_connected", currentConnCollector, currentTimeStamp)
				collectStatus(newStatusMap, "Max_used_connections", maxUsedConnCollector, currentTimeStamp)
				collectStatus(newSetupMap, "max_connections", maxConnCollector, currentTimeStamp)

				collectConnUsage(newStatusMap, newSetupMap, connUsageCollector, currentTimeStamp)
			}

			prevStatusMap = newStatusMap
		}

		util.SleepInterval(conf.CollectInterval, startTime)
	}
}

func openMySQL(conf *config.MySQLConfig) *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", conf.Username, conf.Password, conf.Host, conf.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		newlog.Error("open mysql for %s failed: %v", dsn, err)
		return nil
	}
	err = db.Ping()
	if err != nil {
		newlog.Error("open mysql for %s failed: %v", dsn, err)
		return nil
	}

	return db
}

func queryMySQL(db *sql.DB, querySQL string) map[string]string {
	rows, err := db.Query(querySQL)
	if err != nil {
		newlog.Error("execute sql: %s failed: %v", querySQL, err)
		return nil
	}

	statusMap := make(map[string]string)
	for rows.Next() {
		var name string
		var value string
		err = rows.Scan(&name, &value)
		if err != nil {
			newlog.Error("scan err: %v", err)
		} else {
			statusMap[name] = value
		}
	}

	return statusMap
}

func collectCounter(prevMap, currentMap map[string]string, key string, collector sentry.Collector, currentTimestamp int64) {
	prevValueStr, ok1 := prevMap[key]
	currentValueStr, ok2 := currentMap[key]
	if ok1 && ok2 {
		prevValue, e1 := strconv.ParseInt(prevValueStr, 10, 64)
		currentValue, e2 := strconv.ParseInt(currentValueStr, 10, 64)
		if e1 != nil || e2 != nil {
			newlog.Error("parseInt failed: %v, %v", e1, e2)
			return
		}

		count := currentValue - prevValue
		collector.PutWithTime(float64(count), currentTimestamp)
	}
}

func collectStatus(currentMap map[string]string, key string, collector sentry.Collector, currentTimestamp int64) {
	if currentValueStr, ok := currentMap[key]; ok {
		currentValue, err := strconv.ParseInt(currentValueStr, 10, 64)
		if err != nil {
			newlog.Error("parseInt failed: %v", err)
			return
		}

		collector.PutWithTime(float64(currentValue), currentTimestamp)
	}
}

func collectConnUsage(statusMap, setupMap map[string]string, collector sentry.Collector, currentTimestamp int64) {
	currentConnStr, ok1 := statusMap["Threads_connected"]
	maxConnStr, ok2 := setupMap["max_connections"]
	if ok1 && ok2 {
		currentConn, e1 := strconv.ParseInt(currentConnStr, 10, 64)
		maxConn, e2 := strconv.ParseInt(maxConnStr, 10, 64)
		if e1 != nil || e2 != nil {
			newlog.Error("parseInt failed: %v, %v", e1, e2)
			return
		}

		connUsage := float64(currentConn*100) / float64(maxConn)
		collector.PutWithTime(connUsage, currentTimestamp)
	}
}