package log

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hermeznetwork/tracerr"
	"github.com/stretchr/testify/assert"
)

func TestLogNotInitialized(t *testing.T) {
	Info("Test log.Info value is ", 10)
	Infof("Test log.Infof %d", 10)
	Infow("Test log.Infow", "value", 10)
	Debug("Test log.Debug value is ", 10)
	Debugf("Test log.Debugf %d", 10)
	Debugw("Test log.Debugw value", 10)
	Error("Test log.Error value is ", 10)
	Errorf("Test log.Errorf %d", 10)
	Errorw("Test log.Errorw value", 10)
	Warn("Test log.Warn value is ", 10)
	Warnf("Test log.Warnf %d", 10)
	Warnw("Test log.Warnw value", 10)
}

func TestLog(t *testing.T) {
	cfg := Config{
		Environment: EnvironmentDevelopment,
		Level:       "debug",
		Outputs:     []string{"stderr"}, // []string{"stdout", "test.log"}
	}

	Init(cfg)

	Info("Test log.Info value is ", 10)
	Infof("Test log.Infof %d", 10)
	Infow("Test log.Infow", "value", 10)
	Debugf("Test log.Debugf %d", 10)
	Error("Test log.Error value is ", 10)
	Errorf("Test log.Errorf %d", 10)
	Errorw("Test log.Errorw", "value", 10)
	Warnf("Test log.Warnf %d", 10)
	Warnw("Test log.Warnw value", 10)
}

func TestLogger_WithFields(t *testing.T) {
	cfg := Config{
		Environment: EnvironmentDevelopment,
		Level:       "debug",
		Outputs:     []string{"stderr"},
	}
	Init(cfg)

	originalLogger := WithFields("originalField", "originalValue")
	derivedLogger := originalLogger.WithFields("newField", "newValue")

	originalCore := originalLogger.x.Desugar().Core()
	assert.NotNil(t, originalCore)
	assert.NotEqual(t, derivedLogger.x, originalLogger.x)

	derivedCore := derivedLogger.x.Desugar().Core()
	assert.NotNil(t, derivedCore)
	assert.NotEqual(t, derivedCore, originalCore)
}

func TestSprintStackTrace(t *testing.T) {
	err := func() error {
		return tracerr.Wrap(func() error {
			return tracerr.New("dummy error")
		}())
	}()

	st := tracerr.StackTrace(err)
	fmt.Println(st)

	stackTraceStr := sprintStackTrace(st)
	fmt.Println(stackTraceStr)

	assert.Contains(t, stackTraceStr, "/log/log_test.go")
	assert.Contains(t, stackTraceStr, "TestSprintStackTrace")
}

func TestAppendStackTraceMaybeArgs(t *testing.T) {
	err := errors.New("test error")
	args := []interface{}{"some value", err}
	newArgs := appendStackTraceMaybeArgs(args)

	assert.Greater(t, len(newArgs), len(args))

	stackTraceStr, ok := newArgs[len(newArgs)-1].(string)
	fmt.Println(stackTraceStr)
	assert.True(t, ok)
	assert.Contains(t, stackTraceStr, "/log/log_test.go")
	assert.Contains(t, stackTraceStr, "TestAppendStackTraceMaybeArgs")
}

func TestAppendStackTraceMaybeKV(t *testing.T) {
	msg := "Test message"

	// Test case: No error in key-value pairs
	kv := []interface{}{"key1", "value1", "key2", "value2"}
	result := appendStackTraceMaybeKV(msg, kv)
	assert.Equal(t, msg, result, "Expected message to be unchanged when no error is present")

	// Test case: Error in key-value pairs
	err := errors.New("Test error")
	wrappedErr := tracerr.Wrap(err)
	expectedErrMsg := fmt.Sprintf("%v: %v", msg, wrappedErr.Error())
	kv = []interface{}{"key1", "value1", "errorKey", err}
	result = appendStackTraceMaybeKV(msg, kv)

	assert.Contains(t, result, expectedErrMsg, "Expected message to include the error and its stack trace")
	assert.Contains(t, result, "log_test.go", "Expected stack trace to include 'log_test.go'")
	assert.Contains(
		t,
		result,
		"TestAppendStackTraceMaybeKV",
		"Expected stack trace to include 'TestAppendStackTraceMaybeKV'",
	)

	// Test case: Error at an even index should be ignored
	kv = []interface{}{err, "value2", "key2", "value2"}
	result = appendStackTraceMaybeKV(msg, kv)
	assert.Equal(t, msg, result, "Expected message to be unchanged when error is at an odd index")
}
