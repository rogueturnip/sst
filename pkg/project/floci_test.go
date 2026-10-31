package project

import (
	"context"
	"strings"
	"testing"

	"github.com/sst/sst/v3/pkg/floci"
)

func TestFlociRejectsUnverifiedProviderBeforeConnecting(t *testing.T) {
	p := &Project{
		floci: &floci.Config{Endpoint: "http://127.0.0.1:1"},
		app:   &App{Providers: map[string]interface{}{"aws": map[string]interface{}{}}},
		lock:  ProviderLock{&ProviderLockEntry{Name: "aws", Version: "7.20.0"}},
	}
	if err := p.loadFlociHome(); err == nil || !strings.Contains(err.Error(), "requires @pulumi/aws 7.48.0") {
		t.Fatalf("got %v", err)
	}
}

func TestFlociRejectsCloudStackCommands(t *testing.T) {
	p := &Project{floci: &floci.Config{Endpoint: "http://127.0.0.1:4566"}}
	for _, command := range []string{"deploy", "diff", "refresh"} {
		if err := p.Run(context.Background(), &StackInput{Command: command}); err == nil || err.Error() != "Floci is only for sst dev" {
			t.Errorf("%s: %v", command, err)
		}
	}
}

func TestCloudCommandDoesNotInheritFlociSettings(t *testing.T) {
	t.Setenv("SST_FLOCI_BRIDGE", "http://host.docker.internal:12345")
	t.Setenv("SST_FLOCI_ENDPOINT", "http://127.0.0.1:4566")
	t.Setenv("AWS_PROFILE", "cloud-test")
	env := (&Project{}).commandEnv()
	if strings.Contains(strings.Join(env, "\n"), "SST_FLOCI") {
		t.Fatal("cloud child inherited Floci settings")
	}
	if !strings.Contains(strings.Join(env, "\n"), "AWS_PROFILE=cloud-test") {
		t.Fatal("cloud credentials changed")
	}
}

func TestFlociCommandDoesNotInheritPreflightControl(t *testing.T) {
	t.Setenv("SST_FLOCI_PREFLIGHT", "1")
	t.Setenv("SST_FLOCI_BRIDGE", "http://stale:1234")
	p := &Project{floci: &floci.Config{Endpoint: "http://127.0.0.1:4567"}}
	env := strings.Join(p.commandEnv(), "\n")
	if strings.Contains(env, "SST_FLOCI_PREFLIGHT=") || strings.Contains(env, "SST_FLOCI_BRIDGE=") {
		t.Fatal("child inherited internal control settings")
	}
	if !strings.Contains(env, "SST_FLOCI_ENDPOINT=http://127.0.0.1:4567") {
		t.Fatal("child lost its selected endpoint")
	}
}

func TestFlociRespectsProtectedStage(t *testing.T) {
	p := &Project{floci: &floci.Config{}, app: &App{Protect: true}}
	if err := p.Run(context.Background(), &StackInput{Command: "remove"}); err != ErrProtectedStage {
		t.Fatalf("got %v", err)
	}
}
