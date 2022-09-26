package filer

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
	"time"
)

type RedisClientOption struct {
	Addr     string
	Password string
}

func RedisClientProvider(o *RedisClientOption) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:               o.Addr,
		Password:           o.Password, // no password set IRisiI8m8JElsvJ7aChgWPcv1lwRGdTu
		DB:                 0,          // use default DB
		MaxRetries:         5,
		DialTimeout:        10 * time.Second,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		PoolSize:           15,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        500 * time.Millisecond,
		IdleCheckFrequency: 500 * time.Millisecond,
	})
	return client, client.Ping().Err()
}

const (
	QuotaSizePrefix  = "quota-size"
	QuotaInodePrefix = "quota-inode"
	UsedSizePrefix   = "size"
	UsedInodePrefix  = "inode"
)

type QuotaPlugin struct {
	client *redis.Client
}

func NewQuotaPluginProvider(client *redis.Client) *QuotaPlugin {
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

	return p.client.HSet(defineRedisKey(path), QuotaSizePrefix, size).Err()
}

func (p *QuotaPlugin) QuotaInodeSet(path string, size int64) (err error) {
	if size == 0 {
		return
	}

	return p.client.HSet(defineRedisKey(path), QuotaInodePrefix, size).Err()
}

func (p *QuotaPlugin) SizeIncrement(path string, size int64) (res int64, err error) {
	if size == 0 {
		return
	}

	return p.client.HIncrBy(defineRedisKey(path), UsedSizePrefix, size).Result()
}

func (p *QuotaPlugin) InodeIncrement(path string, size int64) (res int64, err error) {
	if size == 0 {
		return
	}

	return p.client.HIncrBy(defineRedisKey(path), UsedInodePrefix, size).Result()
}

func (p *QuotaPlugin) GetAll(path string) (quotaSize, quotaInode, size, inode int64, err error) {
	res, err := p.client.HGetAll(defineRedisKey(path)).Result()
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
