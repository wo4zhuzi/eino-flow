package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/compose"
)

var (
	// ErrNilContext 表示构建或运行时没有可用的 context。
	ErrNilContext = errors.New("context 不能为空")
	// ErrInvalidName 表示工作流没有稳定名称。
	ErrInvalidName = errors.New("工作流名称无效")
	// ErrInvalidVersion 表示工作流没有稳定版本。
	ErrInvalidVersion = errors.New("工作流版本无效")
	// ErrNilDefinition 表示没有传入可编译的 Chain 或 Graph。
	ErrNilDefinition = errors.New("工作流定义不能为空")
	// ErrRunnerNotInitialized 表示 Runner 尚未成功构建。
	ErrRunnerNotInitialized = errors.New("工作流 Runner 未初始化")
	// ErrNilDependency 表示业务工作流缺少必需依赖。
	ErrNilDependency = errors.New("工作流依赖不能为空")
	// ErrNilCompileOption 表示公共编译入口收到了 nil Option。
	ErrNilCompileOption = errors.New("工作流编译 Option 不能为空")
	// ErrNilRunOption 表示运行入口收到了 nil Option。
	ErrNilRunOption = errors.New("工作流运行 Option 不能为空")
	// ErrInvalidRunID 表示一次执行没有稳定标识。
	ErrInvalidRunID = errors.New("工作流 RunID 无效")
	// ErrNilObserver 表示运行时注入了不可用的 Observer。
	ErrNilObserver = errors.New("工作流 Observer 不能为空")
	// ErrInvalidRuntimeMaxSteps 表示运行级最大步数不是正数。
	ErrInvalidRuntimeMaxSteps = errors.New("工作流运行最大步数无效")
)

// Operation 表示工作流错误发生的生命周期阶段。
type Operation string

const (
	OperationCompile Operation = "compile"
	OperationRun     Operation = "run"
)

// Descriptor 是用于日志、指标、灰度和兼容判断的稳定工作流身份。
type Descriptor struct {
	Name    string
	Version string
}

// String 返回可用于 GraphName 和日志字段的稳定标识。
func (d Descriptor) String() string {
	if d.Name == "" {
		return ""
	}
	return d.Name + "@" + d.Version
}

func (d Descriptor) normalized() (Descriptor, error) {
	d.Name = strings.TrimSpace(d.Name)
	d.Version = strings.TrimSpace(d.Version)
	if d.Name == "" {
		return Descriptor{}, ErrInvalidName
	}
	if d.Version == "" {
		return Descriptor{}, ErrInvalidVersion
	}
	return d, nil
}

// Execution 描述一次可观测的工作流执行。
type Execution struct {
	Descriptor Descriptor
	RunID      string
}

// OperationError 为基础错误补充工作流身份、执行标识和生命周期阶段。
type OperationError struct {
	Execution Execution
	Operation Operation
	Cause     error
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	identity := e.Execution.Descriptor.String()
	if e.Execution.RunID == "" {
		return fmt.Sprintf("%s 工作流 %q: %v", e.Operation, identity, e.Cause)
	}
	return fmt.Sprintf(
		"%s 工作流 %q run_id=%q: %v",
		e.Operation,
		identity,
		e.Execution.RunID,
		e.Cause,
	)
}

// Unwrap 保留 context、业务错误和 Eino 错误链。
func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Observer 把业务无关的运行观测转换为 Eino Callback。
type Observer interface {
	Handler(execution Execution) callbacks.Handler
}

// RunOption 配置单次执行，不改变已编译拓扑。
type RunOption func(*runOptions) error

type runOptions struct {
	runID           string
	observers       []Observer
	runtimeMaxSteps int
}

// WithRunID 设置由调用入口生成或传入的稳定执行标识。
func WithRunID(runID string) RunOption {
	return func(options *runOptions) error {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			return ErrInvalidRunID
		}
		options.runID = runID
		return nil
	}
}

// WithObserver 为本次执行增加日志、Trace 或指标观察器。
func WithObserver(observer Observer) RunOption {
	return func(options *runOptions) error {
		if isNilValue(observer) {
			return ErrNilObserver
		}
		options.observers = append(options.observers, observer)
		return nil
	}
}

// WithRuntimeMaxSteps 为本次执行设置比编译配置更具体的步数保护。
func WithRuntimeMaxSteps(maxSteps int) RunOption {
	return func(options *runOptions) error {
		if maxSteps < 1 {
			return ErrInvalidRuntimeMaxSteps
		}
		options.runtimeMaxSteps = maxSteps
		return nil
	}
}

// Compilable 是 compose.Chain 和 compose.Graph 共有的最小编译能力。
type Compilable[I, O any] interface {
	Compile(
		ctx context.Context,
		opts ...compose.GraphCompileOption,
	) (compose.Runnable[I, O], error)
}

// Runner 保存一次编译得到的 Runnable，并提供可治理运行入口。
type Runner[I, O any] struct {
	descriptor Descriptor
	runnable   compose.Runnable[I, O]
}

// Compile 在启动阶段编译一次 Chain 或 Graph。
func Compile[I, O any](
	ctx context.Context,
	descriptor Descriptor,
	definition Compilable[I, O],
	opts ...compose.GraphCompileOption,
) (*Runner[I, O], error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	normalized, err := descriptor.normalized()
	if err != nil {
		return nil, err
	}
	if isNilValue(definition) {
		return nil, ErrNilDefinition
	}
	for index, option := range opts {
		if option == nil {
			return nil, fmt.Errorf("%w: 第 %d 个 Option", ErrNilCompileOption, index+1)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, newOperationError(normalized, "", OperationCompile, err)
	}

	compileOptions := append([]compose.GraphCompileOption(nil), opts...)
	compileOptions = append(compileOptions, compose.WithGraphName(normalized.String()))
	runnable, err := definition.Compile(ctx, compileOptions...)
	if err != nil {
		return nil, newOperationError(normalized, "", OperationCompile, err)
	}
	if runnable == nil {
		return nil, newOperationError(normalized, "", OperationCompile, ErrRunnerNotInitialized)
	}
	return &Runner[I, O]{descriptor: normalized, runnable: runnable}, nil
}

// Run 使用已编译的 Runnable 执行一次带稳定 RunID 的请求。
func (r *Runner[I, O]) Run(ctx context.Context, input I, opts ...RunOption) (O, error) {
	var zero O
	if ctx == nil {
		return zero, ErrNilContext
	}
	if r == nil || r.runnable == nil {
		return zero, ErrRunnerNotInitialized
	}

	config, err := resolveRunOptions(opts)
	if err != nil {
		return zero, err
	}
	execution := Execution{Descriptor: r.descriptor, RunID: config.runID}
	if err := ctx.Err(); err != nil {
		return zero, newOperationError(r.descriptor, config.runID, OperationRun, err)
	}

	composeOptions := make([]compose.Option, 0, 2)
	if len(config.observers) > 0 {
		handlers := make([]callbacks.Handler, 0, len(config.observers))
		for _, observer := range config.observers {
			handler := observer.Handler(execution)
			if isNilValue(handler) {
				return zero, ErrNilObserver
			}
			handlers = append(handlers, handler)
		}
		composeOptions = append(composeOptions, compose.WithCallbacks(handlers...))
	}
	if config.runtimeMaxSteps > 0 {
		composeOptions = append(composeOptions, compose.WithRuntimeMaxSteps(config.runtimeMaxSteps))
	}

	output, err := r.runnable.Invoke(ctx, input, composeOptions...)
	if err != nil {
		return zero, newOperationError(r.descriptor, config.runID, OperationRun, err)
	}
	return output, nil
}

// Descriptor 返回工作流的稳定名称和版本。
func (r *Runner[I, O]) Descriptor() Descriptor {
	if r == nil {
		return Descriptor{}
	}
	return r.descriptor
}

// RequireDependency 校验依赖接口，包括接口中包含 typed nil 指针的情况。
func RequireDependency(name string, value any) error {
	if !isNilValue(value) {
		return nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "dependency"
	}
	return fmt.Errorf("%w: %s", ErrNilDependency, name)
}

func resolveRunOptions(opts []RunOption) (runOptions, error) {
	var config runOptions
	for index, option := range opts {
		if option == nil {
			return runOptions{}, fmt.Errorf("%w: 第 %d 个 Option", ErrNilRunOption, index+1)
		}
		if err := option(&config); err != nil {
			return runOptions{}, err
		}
	}
	if strings.TrimSpace(config.runID) == "" {
		return runOptions{}, ErrInvalidRunID
	}
	return config, nil
}

func newOperationError(
	descriptor Descriptor,
	runID string,
	operation Operation,
	cause error,
) *OperationError {
	return &OperationError{
		Execution: Execution{Descriptor: descriptor, RunID: runID},
		Operation: operation,
		Cause:     cause,
	}
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
