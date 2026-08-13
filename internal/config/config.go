// Package config 负责从进程环境加载并校验应用运行配置。
package config

import (
	"fmt"
	"io"
	"time"
)

const redacted = "[REDACTED]"

// Secret 保存必须显式取用的敏感配置，并阻止默认格式化泄漏原值。
type Secret struct {
	value string
}

// Reveal 返回敏感配置原值。调用方不得记录返回值。
func (s Secret) Reveal() string {
	return s.value
}

func (Secret) String() string {
	return redacted
}

func (Secret) GoString() string {
	return redacted
}

// Format 保证所有 fmt 格式化动词都只输出脱敏占位符。
func (Secret) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, redacted)
}

// Config 是应用启动时一次性加载的完整运行配置。
// 所有字段保持私有，避免加载后被业务代码直接修改。
type Config struct {
	postgres  PostgreSQL
	embedding Embedding
}

// PostgreSQL 返回数据库配置的只读副本。
func (c Config) PostgreSQL() PostgreSQL {
	return c.postgres
}

// Embedding 返回 Embedding 配置的只读副本。
func (c Config) Embedding() Embedding {
	return c.embedding
}

func (c Config) String() string {
	return fmt.Sprintf("Config{PostgreSQL:%s Embedding:%s}", c.postgres, c.embedding)
}

func (c Config) GoString() string {
	return c.String()
}

// PostgreSQL 描述数据库地址、作用 schema 和连接池参数。
type PostgreSQL struct {
	host            string
	port            int
	user            string
	password        Secret
	database        string
	sslMode         string
	schema          string
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
}

func (c PostgreSQL) Host() string                   { return c.host }
func (c PostgreSQL) Port() int                      { return c.port }
func (c PostgreSQL) User() string                   { return c.user }
func (c PostgreSQL) Password() Secret               { return c.password }
func (c PostgreSQL) Database() string               { return c.database }
func (c PostgreSQL) SSLMode() string                { return c.sslMode }
func (c PostgreSQL) Schema() string                 { return c.schema }
func (c PostgreSQL) MaxOpenConns() int              { return c.maxOpenConns }
func (c PostgreSQL) MaxIdleConns() int              { return c.maxIdleConns }
func (c PostgreSQL) ConnMaxLifetime() time.Duration { return c.connMaxLifetime }

func (c PostgreSQL) String() string {
	return fmt.Sprintf(
		"PostgreSQL{Host:%q Port:%d User:%q Password:%s Database:%q SSLMode:%q Schema:%q MaxOpenConns:%d MaxIdleConns:%d ConnMaxLifetime:%s}",
		c.host,
		c.port,
		c.user,
		redacted,
		c.database,
		c.sslMode,
		c.schema,
		c.maxOpenConns,
		c.maxIdleConns,
		c.connMaxLifetime,
	)
}

func (c PostgreSQL) GoString() string {
	return c.String()
}

// Embedding 描述 OpenAI 兼容 Embedding API 的连接、模型和并发边界。
type Embedding struct {
	baseURL    string
	key        Secret
	model      string
	dimensions int
	timeout    time.Duration
	batchSize  int
}

func (c Embedding) BaseURL() string        { return c.baseURL }
func (c Embedding) Key() Secret            { return c.key }
func (c Embedding) Model() string          { return c.model }
func (c Embedding) Dimensions() int        { return c.dimensions }
func (c Embedding) Timeout() time.Duration { return c.timeout }
func (c Embedding) BatchSize() int         { return c.batchSize }

func (c Embedding) String() string {
	return fmt.Sprintf(
		"Embedding{BaseURL:%q Key:%s Model:%q Dimensions:%d Timeout:%s BatchSize:%d}",
		c.baseURL,
		redacted,
		c.model,
		c.dimensions,
		c.timeout,
		c.batchSize,
	)
}

func (c Embedding) GoString() string {
	return c.String()
}
