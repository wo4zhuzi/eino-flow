package workflow

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/compose"
)

type stringWorkflowDefinition struct {
	compileCalls atomic.Int64
	handlerCalls atomic.Int64
	compileErr   error
	runErr       error
}

func (d *stringWorkflowDefinition) Compile(
	ctx context.Context,
	opts ...compose.GraphCompileOption,
) (compose.Runnable[string, string], error) {
	d.compileCalls.Add(1)
	if d.compileErr != nil {
		return nil, d.compileErr
	}
	chain := compose.NewChain[string, string]()
	chain.AppendLambda(
		compose.InvokableLambda(func(_ context.Context, input string) (string, error) {
			d.handlerCalls.Add(1)
			if d.runErr != nil {
				return "", d.runErr
			}
			return strings.ToUpper(input), nil
		}),
		compose.WithNodeKey("uppercase"),
		compose.WithNodeName("uppercase"),
	)
	return chain.Compile(ctx, opts...)
}

func TestCompileOnceRunManyAndObserve(t *testing.T) {
	definition := &stringWorkflowDefinition{}
	descriptor := Descriptor{Name: "uppercase", Version: "v1"}
	runner, err := Compile(context.Background(), descriptor, definition)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	recorder := NewRecorder()

	for index, input := range []string{"first", "second"} {
		runID := "run-" + string(rune('1'+index))
		output, err := runner.Run(
			context.Background(),
			input,
			WithRunID(runID),
			WithObserver(recorder),
		)
		if err != nil {
			t.Fatalf("Run(%q) error = %v", input, err)
		}
		if output != strings.ToUpper(input) {
			t.Fatalf("Run(%q) = %q", input, output)
		}
	}
	if definition.compileCalls.Load() != 1 || definition.handlerCalls.Load() != 2 {
		t.Fatalf(
			"compileCalls=%d handlerCalls=%d, want 1 and 2",
			definition.compileCalls.Load(),
			definition.handlerCalls.Load(),
		)
	}
	if runner.Descriptor() != descriptor {
		t.Fatalf("Descriptor() = %#v", runner.Descriptor())
	}

	events := recorder.Events()
	if !hasEvent(events, "run-1", "uppercase", StatusSucceeded) ||
		!hasEvent(events, "run-2", "uppercase", StatusSucceeded) {
		t.Fatalf("observer events = %#v", events)
	}
}

func TestCompileValidatesAndPreservesErrors(t *testing.T) {
	errCompile := errors.New("compile failed")
	descriptor := Descriptor{Name: "broken", Version: "v2"}
	_, err := Compile(context.Background(), descriptor, &stringWorkflowDefinition{compileErr: errCompile})
	if !errors.Is(err, errCompile) {
		t.Fatalf("Compile() error = %v, want errCompile", err)
	}
	var operationError *OperationError
	if !errors.As(err, &operationError) ||
		operationError.Operation != OperationCompile ||
		operationError.Execution.Descriptor != descriptor {
		t.Fatalf("Compile() OperationError = %#v", operationError)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name       string
		ctx        context.Context
		descriptor Descriptor
		definition Compilable[string, string]
		want       error
	}{
		{name: "nil context", descriptor: descriptor, definition: &stringWorkflowDefinition{}, want: ErrNilContext},
		{name: "empty name", ctx: context.Background(), descriptor: Descriptor{Version: "v1"}, definition: &stringWorkflowDefinition{}, want: ErrInvalidName},
		{name: "empty version", ctx: context.Background(), descriptor: Descriptor{Name: "workflow"}, definition: &stringWorkflowDefinition{}, want: ErrInvalidVersion},
		{name: "nil definition", ctx: context.Background(), descriptor: descriptor, want: ErrNilDefinition},
		{name: "canceled context", ctx: canceled, descriptor: descriptor, definition: &stringWorkflowDefinition{}, want: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Compile(test.ctx, test.descriptor, test.definition)
			if !errors.Is(err, test.want) {
				t.Fatalf("Compile() error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := Compile(
		context.Background(),
		descriptor,
		&stringWorkflowDefinition{},
		compose.GraphCompileOption(nil),
	); !errors.Is(err, ErrNilCompileOption) {
		t.Fatalf("Compile(nil option) error = %v", err)
	}
}

func TestRunValidatesOptionsAndPreservesErrors(t *testing.T) {
	errRun := errors.New("dependency unavailable")
	descriptor := Descriptor{Name: "failing", Version: "v3"}
	runner, err := Compile(
		context.Background(),
		descriptor,
		&stringWorkflowDefinition{runErr: errRun},
	)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	_, err = runner.Run(context.Background(), "input", WithRunID("request-42"))
	if !errors.Is(err, errRun) {
		t.Fatalf("Run() error = %v, want errRun", err)
	}
	var operationError *OperationError
	if !errors.As(err, &operationError) ||
		operationError.Operation != OperationRun ||
		operationError.Execution.RunID != "request-42" ||
		operationError.Execution.Descriptor != descriptor {
		t.Fatalf("Run() OperationError = %#v", operationError)
	}

	validRunner, err := Compile(
		context.Background(),
		Descriptor{Name: "valid", Version: "v1"},
		&stringWorkflowDefinition{},
	)
	if err != nil {
		t.Fatalf("Compile() valid runner error = %v", err)
	}
	var nilObserver *Recorder
	tests := []struct {
		name string
		ctx  context.Context
		opts []RunOption
		want error
	}{
		{name: "nil context", opts: []RunOption{WithRunID("run")}, want: ErrNilContext},
		{name: "missing run id", ctx: context.Background(), want: ErrInvalidRunID},
		{name: "empty run id", ctx: context.Background(), opts: []RunOption{WithRunID(" ")}, want: ErrInvalidRunID},
		{name: "nil option", ctx: context.Background(), opts: []RunOption{nil}, want: ErrNilRunOption},
		{name: "nil observer", ctx: context.Background(), opts: []RunOption{WithRunID("run"), WithObserver(nilObserver)}, want: ErrNilObserver},
		{name: "invalid max steps", ctx: context.Background(), opts: []RunOption{WithRunID("run"), WithRuntimeMaxSteps(0)}, want: ErrInvalidRuntimeMaxSteps},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validRunner.Run(test.ctx, "input", test.opts...)
			if !errors.Is(err, test.want) {
				t.Fatalf("Run() error = %v, want %v", err, test.want)
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = validRunner.Run(canceled, "input", WithRunID("canceled-run"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) error = %v", err)
	}
}

func TestRunnerCanRunConcurrently(t *testing.T) {
	definition := &stringWorkflowDefinition{}
	runner, err := Compile(
		context.Background(),
		Descriptor{Name: "concurrent", Version: "v1"},
		definition,
	)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	recorder := NewRecorder()

	const runs = 24
	errCh := make(chan error, runs)
	var wg sync.WaitGroup
	for index := 0; index < runs; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			output, err := runner.Run(
				context.Background(),
				"input",
				WithRunID(fmtRunID(index)),
				WithObserver(recorder),
			)
			if err != nil {
				errCh <- err
				return
			}
			if output != "INPUT" {
				errCh <- errors.New("unexpected output")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if definition.compileCalls.Load() != 1 || definition.handlerCalls.Load() != runs {
		t.Fatalf(
			"compileCalls=%d handlerCalls=%d, want 1 and %d",
			definition.compileCalls.Load(),
			definition.handlerCalls.Load(),
			runs,
		)
	}
	for index := 0; index < runs; index++ {
		if !hasEvent(recorder.Events(), fmtRunID(index), "uppercase", StatusSucceeded) {
			t.Fatalf("missing successful event for %s", fmtRunID(index))
		}
	}
}

func TestRequireDependencyRejectsTypedNil(t *testing.T) {
	var definition *stringWorkflowDefinition
	err := RequireDependency("definition", definition)
	if !errors.Is(err, ErrNilDependency) || !strings.Contains(err.Error(), "definition") {
		t.Fatalf("RequireDependency() error = %v", err)
	}
	if err := RequireDependency("definition", &stringWorkflowDefinition{}); err != nil {
		t.Fatalf("RequireDependency() error = %v", err)
	}
}

type nilHandlerObserver struct{}

func (*nilHandlerObserver) Handler(Execution) callbacks.Handler {
	return nil
}

func TestRunRejectsObserverReturningNilHandler(t *testing.T) {
	runner, err := Compile(
		context.Background(),
		Descriptor{Name: "observer", Version: "v1"},
		&stringWorkflowDefinition{},
	)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	_, err = runner.Run(
		context.Background(),
		"input",
		WithRunID("run"),
		WithObserver(&nilHandlerObserver{}),
	)
	if !errors.Is(err, ErrNilObserver) {
		t.Fatalf("Run() error = %v, want ErrNilObserver", err)
	}
}

func hasEvent(events []Event, runID, name, status string) bool {
	for _, event := range events {
		if event.Execution.RunID == runID && event.Name == name && event.Status == status {
			return true
		}
	}
	return false
}

func fmtRunID(index int) string {
	return "run-" + strconv.Itoa(index+1)
}
