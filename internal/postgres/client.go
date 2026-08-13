// Package postgres 管理 PostgreSQL 通用连接、连接池和生命周期。
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	postgresdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	// ErrNilContext 表示连接操作没有可用的 context。
	ErrNilContext = errors.New("PostgreSQL context 不能为空")
	// ErrOpen 表示无法初始化 GORM 或底层 SQL 连接。
	ErrOpen = errors.New("打开 PostgreSQL 连接失败")
	// ErrPing 表示 PostgreSQL 启动连通性检查失败。
	ErrPing = errors.New("PostgreSQL Ping 失败")
	// ErrUnavailable 表示 PostgreSQL Client 尚未成功初始化。
	ErrUnavailable = errors.New("PostgreSQL Client 不可用")
)

// OperationError 隐藏底层错误文本，避免连接串或密码进入默认错误输出。
// Unwrap 仍保留错误链，供 errors.Is 和 errors.As 判断。
type OperationError struct {
	operation error
	cause     error
}

func (e *OperationError) Error() string {
	if e == nil || e.operation == nil {
		return ""
	}
	return e.operation.Error()
}

func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Is 允许调用方同时按操作分类错误和判断底层原因。
func (e *OperationError) Is(target error) bool {
	return e != nil && target == e.operation
}

// Client 保存可复用的 GORM 句柄及其底层连接池。
type Client struct {
	db    *gorm.DB
	sqlDB *sql.DB

	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
}

// Open 创建连接、配置连接池并完成启动 Ping。
func Open(ctx context.Context, config appconfig.PostgreSQL) (*Client, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	dialector := postgresdriver.New(postgresdriver.Config{
		DSN: connectionURL(config),
	})
	return open(ctx, config, dialector)
}

func open(
	ctx context.Context,
	config appconfig.PostgreSQL,
	dialector gorm.Dialector,
) (*Client, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, operationError(ErrOpen, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, operationError(ErrOpen, err)
	}
	sqlDB.SetMaxOpenConns(config.MaxOpenConns())
	sqlDB.SetMaxIdleConns(config.MaxIdleConns())
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime())

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, operationError(ErrPing, err)
	}
	return &Client{db: db, sqlDB: sqlDB}, nil
}

// DB 返回供启动层注入具体基础设施实现的 GORM 句柄。
func (c *Client) DB() (*gorm.DB, error) {
	if c == nil || c.db == nil || c.sqlDB == nil || c.closed.Load() {
		return nil, ErrUnavailable
	}
	return c.db, nil
}

// Ping 检查现有连接池能否访问 PostgreSQL。
func (c *Client) Ping(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if c == nil || c.sqlDB == nil || c.closed.Load() {
		return ErrUnavailable
	}
	if err := c.sqlDB.PingContext(ctx); err != nil {
		return operationError(ErrPing, err)
	}
	return nil
}

// Close 关闭底层连接池；重复调用返回第一次关闭的结果。
func (c *Client) Close() error {
	if c == nil || c.sqlDB == nil {
		return ErrUnavailable
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.sqlDB.Close()
		c.closed.Store(true)
	})
	return c.closeErr
}

func connectionURL(config appconfig.PostgreSQL) string {
	query := make(url.Values, 1)
	query.Set("sslmode", config.SSLMode())
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(config.User(), config.Password().Reveal()),
		Host:     net.JoinHostPort(config.Host(), strconv.Itoa(config.Port())),
		Path:     config.Database(),
		RawQuery: query.Encode(),
	}).String()
}

func operationError(operation, cause error) error {
	if cause == nil {
		return operation
	}
	return &OperationError{operation: operation, cause: cause}
}

func (e *OperationError) GoString() string {
	return fmt.Sprintf("OperationError{%s}", e.Error())
}
