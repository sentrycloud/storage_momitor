package main

import (
	"github.com/sentrycloud/sentry/pkg/cmdflags"
	"github.com/sentrycloud/sentry/pkg/newlog"
	"github.com/sentrycloud/storage_momitor/pkg/config"
	"github.com/sentrycloud/storage_momitor/pkg/mysql"
	"github.com/sentrycloud/storage_momitor/pkg/redis"
	"time"
)

func main() {
	newlog.Info("start sentry storage server monitor")

	// parse command parameters
	var cmdParams = cmdflags.CmdParams{}
	cmdParams.Parse("storageServerConfig.yaml")

	// parse config file
	err := config.Parse(cmdParams.ConfigPath)
	if err != nil {
		return
	}

	// set log level, path, max file size and max file backups
	newlog.SetConfig(&config.ServerConfig.Log)

	// start redis monitor
	redis.StartMonitor()

	// start MySQL monitor
	mysql.StartMonitor()

	// enter the endless loop
	for {
		time.Sleep(1 * time.Second)
	}
}
