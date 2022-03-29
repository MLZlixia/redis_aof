package persistence

import (
	"errors"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"gopkg.in/yaml.v2"
)

var redisConfig RedisConfig

const (
	singleInsMod = 1 // 单实例
	clusterMod   = 2 // 集群节点
	sentinelMod  = 3 // 哨兵
)

func ReadConfig(filePath string) error {
	log.Printf("read config info %s\n", filePath)
	configByte, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(configByte, &redisConfig); err != nil {
		log.Fatalf("read  config err: %s\n", err.Error())
		return err
	}

	if redisConfig.Hosts == "" {
		return errors.New("must have db hosts")
	}

	if redisConfig.PoolSize == 0 {
		redisConfig.PoolSize = 50
	}

	if redisConfig.MaxRetries == 0 {
		redisConfig.MaxRetries = 3
	}

	if redisConfig.IdleConns == 0 {
		redisConfig.IdleConns = 10
	}

	if redisConfig.Timeout == 0 {
		redisConfig.Timeout = 30
	}

	if redisConfig.IdleTime == 0 {
		redisConfig.IdleTime = 1
	}

	if redisConfig.LifeTime == 0 {
		redisConfig.LifeTime = 0
	}

	redisConfig.Timeout = redisConfig.Timeout * time.Second
	redisConfig.IdleTime = redisConfig.IdleTime * time.Hour
	redisConfig.LifeTime = redisConfig.LifeTime * time.Hour

	if redisConfig.DBMod < 1 {
		redisConfig.DBMod = 1
	}

	if redisConfig.DB < 0 {
		redisConfig.DB = 0
	}

	return nil
}

type RedisConfig struct {
	Hosts      string        `yaml:"hosts"` // 使用逗号隔开
	Password   string        `yaml:"password"`
	DB         int           `yaml:"db"`
	Timeout    time.Duration `yaml:"timeout"`
	PoolSize   int           `yaml:"pool_size"`
	MaxRetries int           `yaml:"max_retries"`
	IdleConns  int           `yaml:"idle_conns"`
	IdleTime   time.Duration `yaml:"idle_time"`
	LifeTime   time.Duration `yaml:"life_time"`
	DBMod      int           `yaml:"db_mod"` // redis模式 1 单例 2 主从 3 哨兵
}

func Open() ([]redis.Cmdable, error) {
	hosts := strings.Split(redisConfig.Hosts, ",")
	clients := make([]redis.Cmdable, len(hosts))
	for index, host := range hosts {
		log.Printf("open db host %s \n", host)
		var client redis.Cmdable

		if redisConfig.DBMod == singleInsMod {
			client = redis.NewClient(&redis.Options{
				Addr:         host,
				Password:     redisConfig.Password,
				DB:           redisConfig.DB,
				DialTimeout:  redisConfig.Timeout,
				PoolSize:     redisConfig.PoolSize,
				MaxRetries:   redisConfig.MaxRetries,
				MinIdleConns: redisConfig.IdleConns,
				IdleTimeout:  redisConfig.IdleTime,
				MaxConnAge:   redisConfig.LifeTime,
			})
		}

		if redisConfig.DBMod == clusterMod {
			client = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:        []string{host},
				Password:     redisConfig.Password,
				DialTimeout:  redisConfig.Timeout,
				PoolSize:     redisConfig.PoolSize,
				MaxRetries:   redisConfig.MaxRetries,
				MinIdleConns: redisConfig.IdleConns,
				IdleTimeout:  redisConfig.IdleTime,
				MaxConnAge:   redisConfig.LifeTime,
			})
		}
		if _, err := client.Ping().Result(); err != nil {
			return nil, err
		}
		clients[index] = client
	}

	return clients, nil
}
