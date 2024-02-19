package config

import (
	"fmt"
	"github.com/sentrycloud/sentry/pkg/newlog"
	"gopkg.in/yaml.v3"
	"os"
)

var ServerConfig StorageServerConfig

type StorageServerConfig struct {
	Log   newlog.LogConfig
	Redis []RedisConfig
	MySQL []MySQLConfig
}

type RedisConfig struct {
	ServerAddr      string `yaml:"server_addr"`
	Password        string `yaml:"password"`
	CollectInterval int64  `yaml:"collect_interval"`
}

type MySQLConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	CollectInterval int64  `yaml:"collect_interval"`
}

func Parse(configPath string) error {
	content, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Printf("read config file failed: %s\n", err)
		return err
	}

	err = yaml.Unmarshal(content, &ServerConfig)
	if err != nil {
		fmt.Printf("unmarshal yaml config failed: %s\n", err)
	}

	return err
}
