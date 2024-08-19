package registry_starter

import (
	goframeworkgoredis "github.com/kordar/goframework-redis"
	logger "github.com/kordar/gologger"
	"github.com/kordar/registry"
	"github.com/kordar/registry-redis"
	"github.com/spf13/cast"
	"time"
)

var (
	HashringRegistryHandle registry.HashringRegistry
	redisnoderegistry      registry.Registry
)

type RegistryRedisModule struct {
}

func (r RegistryRedisModule) Name() string {
	return "registry_starter"
}

func (r RegistryRedisModule) Load(value interface{}) {
	item := cast.ToStringMapString(value)

	if item["prefix"] == "" {
		logger.Fatal("[registry_starter] 请设置正确的prefix参数")
	}
	if item["node"] == "" {
		logger.Fatal("[registry_starter] 请设置正确的node参数")
	}
	if item["channel"] == "" {
		logger.Fatal("[registry_starter] 请设置正确的channel参数")
	}

	redis := cast.ToString(item["redis"])
	if goframeworkgoredis.HasRedisInstance(redis) {
		r.registryRedis(item)
	}

}

func (r RegistryRedisModule) registryRedis(item map[string]string) {

	timeout := time.Second * 300
	if item["timeout"] != "" {
		timeout = cast.ToDuration(item["timeout"]) * time.Second
	}

	heartbeat := time.Second * 60
	if item["heartbeat"] != "" {
		heartbeat = cast.ToDuration(item["heartbeat"]) * time.Second
	}

	if heartbeat >= timeout {
		logger.Fatal("[registry_starter] 心跳设置无效，心跳时间需小于缓存时间")
	}

	virtualSpots := 100
	if item["virtualSpots"] != "" {
		virtualSpots = cast.ToInt(item["virtualSpots"])
	}

	HashringRegistryHandle = registry.NewHashringRegistry(virtualSpots, item["node"])

	client := goframeworkgoredis.GetRedisClient(item["redis"])
	if client == nil {
		logger.Fatal("[registry_starter] 初始化注册中心redis失败")
	}

	redisnoderegistry = registry_redis.NewRedisNodeRegistry(client, &registry_redis.RedisNodeRegistryOptions{
		Prefix:  cast.ToString(item["prefix"]),
		Node:    cast.ToString(item["node"]),
		Channel: cast.ToString(item["channel"]),
		Reload: func(value []string, channel string) {
			HashringRegistryHandle.Load(value)
		},
		Heartbeat: heartbeat,
		Timeout:   timeout,
	})

	redisnoderegistry.Listener()
	if err := redisnoderegistry.Register(); err == nil {
		logger.Infof("[registry_starter] 初始化registry redis成功")
	} else {
		logger.Errorf("[registry_starter] 初始化registry redis异常，err=%v", err)
	}
}

func (r RegistryRedisModule) Close() {
	_ = redisnoderegistry.Remove()
}
