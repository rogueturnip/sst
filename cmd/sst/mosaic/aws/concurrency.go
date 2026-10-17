package aws

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/sst/sst/v3/pkg/bus"
	"github.com/sst/sst/v3/pkg/project"
)

// ConcurrencyWarningEvent says the account can run few Lambda functions at once
// for sst dev.
type ConcurrencyWarningEvent struct {
	Message string
}

// Below this many unreserved concurrent executions, sst dev warns. An account
// gets 1,000 by default, and some new ones only 10.
const lowConcurrency = 100

// concurrencyWarning says why the account's Lambda concurrency is low for sst
// dev, or nothing when it isn't.
func concurrencyWarning(limit *types.AccountLimit) string {
	if limit == nil {
		return ""
	}
	unreserved := limit.ConcurrentExecutions
	if limit.UnreservedConcurrentExecutions != nil {
		unreserved = *limit.UnreservedConcurrentExecutions
	}
	if unreserved >= lowConcurrency {
		return ""
	}
	return fmt.Sprintf("Lambda runs only %d invocations at once in this AWS account, besides reserved concurrency. "+
		"In sst dev, an invocation holds one for as long as it runs on your machine, "+
		"so more requests than that at once fail with a 500 or a 429. "+
		"To raise it, request a higher \"Concurrent executions\" quota for Lambda in Service Quotas.", unreserved)
}

// warnLowConcurrency warns, once the first deploy of sst dev is done, when the
// account's Lambda concurrency is low. The warning waits for the deploy, as
// the terminal that shows it isn't connected before then. When the settings
// can't be read, as without the lambda:GetAccountSettings permission, it says
// nothing.
func warnLowConcurrency(ctx context.Context, settings func(context.Context) (*lambda.GetAccountSettingsOutput, error)) {
	evts := bus.Subscribe(&project.CompleteEvent{})
	defer bus.Unsubscribe(evts)
	out, err := settings(ctx)
	if err != nil {
		slog.Info("could not read the lambda account settings", "err", err)
		return
	}
	message := concurrencyWarning(out.AccountLimit)
	if message == "" {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-evts:
			if complete, ok := evt.(*project.CompleteEvent); ok && !complete.Old {
				bus.Publish(&ConcurrencyWarningEvent{Message: message})
				return
			}
		}
	}
}

func accountSettings(cfg aws.Config) func(context.Context) (*lambda.GetAccountSettingsOutput, error) {
	client := lambda.NewFromConfig(cfg)
	return func(ctx context.Context) (*lambda.GetAccountSettingsOutput, error) {
		return client.GetAccountSettings(ctx, &lambda.GetAccountSettingsInput{})
	}
}
