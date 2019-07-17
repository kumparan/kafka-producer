package kafka

import (
	redigo "github.com/gomodule/redigo/redis"
)

// Option :nodoc:
type Option func(*Options) error

// Options :nodoc:
type Options struct {
	redisConn                             *redigo.Pool
	failedMessagesRedisKey                string
	deadMessagesRedisKey                  string
	failedMessagePublishIntervalInSeconds uint64
}

// WithRedis :nodoc:
func WithRedis(conn *redigo.Pool) Option {
	return func(opt *Options) error {
		opt.redisConn = conn
		return nil
	}
}

// WithFailedMessageRedisKey :nodoc:
func WithFailedMessageRedisKey(key string) Option {
	return func(opt *Options) error {
		opt.failedMessagesRedisKey = key
		return nil
	}
}

// WithDeadMessageRedisKey :nodoc:
func WithDeadMessageRedisKey(key string) Option {
	return func(opt *Options) error {
		opt.deadMessagesRedisKey = key
		return nil
	}
}

// WithFailedMessagePublishInterval :nodoc:
func WithFailedMessagePublishInterval(seconds uint64) Option {
	return func(opt *Options) error {
		opt.failedMessagePublishIntervalInSeconds = seconds
		return nil
	}
}
