package workflow

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cloudwego/eino/callbacks"
)

const (
	StatusStarted   = "started"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// Event 是一次 Eino 组件生命周期事件的稳定快照。
type Event struct {
	Execution Execution
	Component string
	Name      string
	Type      string
	Status    string
	Duration  time.Duration
	ErrorKind string
}

// Recorder 是并发安全的示例 Observer，生产环境可替换为日志、Trace 或指标实现。
type Recorder struct {
	now func() time.Time

	mu     sync.Mutex
	events []Event
}

type startedAtKey struct{}

// NewRecorder 创建使用系统时钟的运行事件记录器。
func NewRecorder() *Recorder {
	return &Recorder{now: time.Now}
}

// Handler 为一次执行创建带 Descriptor 和 RunID 的 Callback。
func (r *Recorder) Handler(execution Execution) callbacks.Handler {
	return callbacks.NewHandlerBuilder().
		OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
			r.add(execution, info, StatusStarted, 0, "")
			return context.WithValue(ctx, startedAtKey{}, r.now())
		}).
		OnEndFn(func(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {
			r.add(execution, info, StatusSucceeded, r.duration(ctx), "")
			return ctx
		}).
		OnErrorFn(func(ctx context.Context, info *callbacks.RunInfo, err error) context.Context {
			r.add(execution, info, StatusFailed, r.duration(ctx), classifyError(err))
			return ctx
		}).
		Build()
}

// Events 返回与内部存储隔离的事件副本。
func (r *Recorder) Events() []Event {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

func (r *Recorder) duration(ctx context.Context) time.Duration {
	startedAt, ok := ctx.Value(startedAtKey{}).(time.Time)
	if !ok {
		return 0
	}
	return r.now().Sub(startedAt)
}

func (r *Recorder) add(
	execution Execution,
	info *callbacks.RunInfo,
	status string,
	duration time.Duration,
	errorKind string,
) {
	event := Event{
		Execution: execution,
		Status:    status,
		Duration:  duration,
		ErrorKind: errorKind,
	}
	if info != nil {
		event.Component = string(info.Component)
		event.Name = info.Name
		event.Type = info.Type
	}
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func classifyError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "internal"
	}
}

var _ Observer = (*Recorder)(nil)
