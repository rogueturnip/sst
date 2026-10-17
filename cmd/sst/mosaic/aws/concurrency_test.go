package aws

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/sst/sst/v3/pkg/bus"
	"github.com/sst/sst/v3/pkg/project"
)

func TestConcurrencyWarning(t *testing.T) {
	cases := []struct {
		name  string
		limit *types.AccountLimit
		warn  string
	}{
		{"no settings", nil, ""},
		{"the default", &types.AccountLimit{ConcurrentExecutions: 1000, UnreservedConcurrentExecutions: aws.Int32(1000)}, ""},
		{"at the threshold", &types.AccountLimit{ConcurrentExecutions: 100, UnreservedConcurrentExecutions: aws.Int32(100)}, ""},
		{"a new account", &types.AccountLimit{ConcurrentExecutions: 10, UnreservedConcurrentExecutions: aws.Int32(10)}, "only 10 invocations"},
		{"most of it reserved", &types.AccountLimit{ConcurrentExecutions: 1000, UnreservedConcurrentExecutions: aws.Int32(50)}, "only 50 invocations"},
		{"unreserved not given", &types.AccountLimit{ConcurrentExecutions: 10}, "only 10 invocations"},
	}
	for _, c := range cases {
		got := concurrencyWarning(c.limit)
		if c.warn == "" && got != "" {
			t.Errorf("%s: warned %q, want nothing", c.name, got)
		}
		if c.warn != "" && !strings.Contains(got, c.warn) {
			t.Errorf("%s: warned %q, want it to say %q", c.name, got, c.warn)
		}
	}
}

// waitForWarning returns the next warning on evts, if one comes within the time.
func waitForWarning(t *testing.T, evts <-chan interface{}, within time.Duration) (*ConcurrencyWarningEvent, bool) {
	t.Helper()
	timeout := time.After(within)
	for {
		select {
		case evt := <-evts:
			if warning, ok := evt.(*ConcurrencyWarningEvent); ok {
				return warning, true
			}
		case <-timeout:
			return nil, false
		}
	}
}

// The warning comes after the first deploy, when the terminal that shows it
// is connected, and only once.
func TestWarnLowConcurrency(t *testing.T) {
	warnings := bus.Subscribe(&ConcurrencyWarningEvent{})
	defer bus.Unsubscribe(warnings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read := make(chan struct{})
	done := make(chan struct{})
	go func() {
		warnLowConcurrency(ctx, func(context.Context) (*lambda.GetAccountSettingsOutput, error) {
			defer close(read)
			return &lambda.GetAccountSettingsOutput{AccountLimit: &types.AccountLimit{ConcurrentExecutions: 10, UnreservedConcurrentExecutions: aws.Int32(10)}}, nil
		})
		close(done)
	}()
	<-read
	if _, ok := waitForWarning(t, warnings, 100*time.Millisecond); ok {
		t.Fatal("warned before the deploy was done")
	}

	bus.Publish(&project.CompleteEvent{Old: true})
	if _, ok := waitForWarning(t, warnings, 100*time.Millisecond); ok {
		t.Fatal("warned on the state of an earlier deploy")
	}

	bus.Publish(&project.CompleteEvent{})
	warning, ok := waitForWarning(t, warnings, 5*time.Second)
	if !ok {
		t.Fatal("didn't warn after the deploy")
	}
	if !strings.Contains(warning.Message, "Service Quotas") {
		t.Fatalf("warning %q doesn't say how to raise it", warning.Message)
	}
	<-done

	bus.Publish(&project.CompleteEvent{})
	if _, ok := waitForWarning(t, warnings, 100*time.Millisecond); ok {
		t.Fatal("warned again after the next deploy")
	}
}

// Without the permission to read the settings, sst dev says nothing.
func TestWarnLowConcurrencyWithoutSettings(t *testing.T) {
	warnings := bus.Subscribe(&ConcurrencyWarningEvent{})
	defer bus.Unsubscribe(warnings)
	done := make(chan struct{})
	go func() {
		warnLowConcurrency(context.Background(), func(context.Context) (*lambda.GetAccountSettingsOutput, error) {
			return nil, errors.New("AccessDeniedException")
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("kept waiting without the settings")
	}
	bus.Publish(&project.CompleteEvent{})
	if _, ok := waitForWarning(t, warnings, 100*time.Millisecond); ok {
		t.Fatal("warned without the settings")
	}
}
