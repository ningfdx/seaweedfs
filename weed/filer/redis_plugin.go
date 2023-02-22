package filer

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	"time"
)

type RedisClientOption struct {
	Addr     []string
	Password string
}

func RedisClientProvider(o *RedisClientOption) (*redis.ClusterClient, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        o.Addr,
		Password:     o.Password,
		MaxRetries:   5,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     15,
		PoolTimeout:  30 * time.Second,
	})

	return client, client.Ping(context.Background()).Err()
}

const (
	QuotaSizePrefix  = "quota-size"
	QuotaInodePrefix = "quota-inode"
	UsedSizePrefix   = "size"
	UsedInodePrefix  = "inode"
)

type QuotaPlugin struct {
	client *redis.ClusterClient
}

func NewQuotaPluginProvider(client *redis.ClusterClient) *QuotaPlugin {
	return &QuotaPlugin{client: client}
}

func defineRedisKey(path string) string {
	path = strings.Trim(path, "/")
	return fmt.Sprintf("adfs-%s", path)
}

func (p *QuotaPlugin) QuotaSizeSet(path string, size int64) (err error) {
	if size == 0 {
		return
	}

	return p.client.HSet(context.Background(), defineRedisKey(path), QuotaSizePrefix, size).Err()
}

func (p *QuotaPlugin) QuotaInodeSet(path string, size int64) (err error) {
	if size == 0 {
		return
	}

	return p.client.HSet(context.Background(), defineRedisKey(path), QuotaInodePrefix, size).Err()
}

func (p *QuotaPlugin) SizeIncrement(path string, size int64) (res int64, err error) {
	if size == 0 {
		return
	}

	return p.client.HIncrBy(context.Background(), defineRedisKey(path), UsedSizePrefix, size).Result()
}

func (p *QuotaPlugin) InodeIncrement(path string, size int64) (res int64, err error) {
	if size == 0 {
		return
	}

	return p.client.HIncrBy(context.Background(), defineRedisKey(path), UsedInodePrefix, size).Result()
}

func (p *QuotaPlugin) GetAll(path string) (quotaSize, quotaInode, size, inode int64, err error) {
	res, err := p.client.HGetAll(context.Background(), defineRedisKey(path)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			err = nil
		}
		return
	}
	for k, v := range res {
		val, _ := strconv.Atoi(v)
		switch k {
		case QuotaSizePrefix:
			quotaSize = int64(val)
		case QuotaInodePrefix:
			quotaInode = int64(val)
		case UsedSizePrefix:
			size = int64(val)
		case UsedInodePrefix:
			inode = int64(val)
		}
	}

	return
}

func defineUserUsageRedisKey(path string) string {
	path = strings.Trim(path, "/")
	return fmt.Sprintf("adfs-usage-%s", path)
}

func (p *QuotaPlugin) PathEveryDayHighestUsageSizeSet(dayPath string, path string, size int64) (err error) {
	if size == 0 {
		return
	}

	dayPath = defineUserUsageRedisKey(dayPath)
	path = strings.Trim(path, "/")

	res, err := p.client.HGet(context.Background(), dayPath, path).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to hget %s-%s, %s", dayPath, path, err.Error())
	}
	val, _ := strconv.Atoi(res)
	if size < int64(val) {
		return nil
	}

	return p.client.HSet(context.Background(), dayPath, path, size).Err()
}
