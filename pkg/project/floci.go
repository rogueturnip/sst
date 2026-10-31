package project

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/sst/sst/v3/internal/util"
	"github.com/sst/sst/v3/pkg/floci"
	"github.com/sst/sst/v3/pkg/global"
	"github.com/sst/sst/v3/pkg/project/provider"
)

func (p *Project) Floci() bool          { return p.floci != nil }
func (p *Project) FlociPreflight() bool { return p.flociChecking.Load() }

func (p *Project) commandEnv() []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "SST_FLOCI" && !strings.HasPrefix(key, "SST_FLOCI_") {
			env = append(env, entry)
		}
	}
	if p.Floci() {
		return provider.FlociEnvironment(env, p.floci.Endpoint)
	}
	return env
}

func (p *Project) loadFlociHome() error {
	for name := range p.app.Providers {
		if name != "aws" {
			return fmt.Errorf("Floci does not support provider %s. No AWS fallback was attempted.", name)
		}
	}
	args, ok := p.app.Providers["aws"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("Floci requires the AWS provider")
	}
	for _, entry := range p.lock {
		if entry.Name == "aws" && entry.Version != "7.48.0" {
			return fmt.Errorf("Floci profile floci-2.3.0-v1 requires @pulumi/aws 7.48.0; found %s. No AWS fallback was attempted.", entry.Version)
		}
	}
	a := provider.NewAwsProvider()
	if err := a.InitFloci(p.app.Name, p.app.Stage, p.floci.Endpoint, args); err != nil {
		return err
	}
	if err := floci.ReadIdentity(context.Background(), p.floci, p.app.Name, p.app.Stage); err != nil {
		return err
	}
	p.loadedProviders = map[string]provider.Provider{"aws": a}
	p.env["SST_FLOCI"] = "1"

	p.env["SST_FLOCI_ENDPOINT"] = p.floci.Endpoint
	for _, entry := range provider.FlociEnvironment(nil, p.floci.Endpoint) {
		k, v, _ := strings.Cut(entry, "=")
		p.env[k] = v
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	p.env["SST_FLOCI_BRIDGE"] = fmt.Sprintf("http://host.docker.internal:%d", port)
	p.env["SST_FLOCI_TOKEN"] = hex.EncodeToString(token)
	p.env["SST_FLOCI_REPORT"] = filepath.Join(p.PathWorkingDir(), "floci-preflight-"+hex.EncodeToString(token[:8])+".json")
	root := p.floci.StateRoot
	if root == "" {
		root = filepath.Join(global.ConfigDir(), "floci-preflight")
	}
	p.home = provider.NewLocalHomeAt(root)
	fmt.Fprintf(os.Stderr, "Floci target: %s | version %s | profile image %s | profile floci-2.3.0-v1 | state %s | stage %s\n", p.floci.Endpoint, floci.Version, floci.Image, root, p.app.Stage)
	return nil
}

func (p *Project) Run(ctx context.Context, input *StackInput) error {
	if !p.Floci() {
		return p.runStack(ctx, input)
	}
	if input.Command == "remove" {
		if p.app.Protect {
			return ErrProtectedStage
		}
		return provider.Purge(p.home, p.app.Name, p.app.Stage)
	}
	if input.Command != "deploy" || !input.Dev {
		return fmt.Errorf("Floci is only for sst dev")
	}
	if len(input.Target) > 0 || len(input.Exclude) > 0 {
		return fmt.Errorf("Floci requires a whole-app preview; targets and exclusions are not supported")
	}
	report := p.env["SST_FLOCI_REPORT"]
	if err := os.Remove(report); err != nil && !os.IsNotExist(err) {
		return err
	}

	p.flociChecking.Store(true)
	preview := *input
	preview.Command = "diff"
	err := p.runStack(ctx, &preview)
	p.flociChecking.Store(false)

	if data, readErr := os.ReadFile(report); readErr == nil {
		var result struct {
			Violations []struct{ Path, Operation, Reason, Fix string }
		}
		if decodeErr := json.Unmarshal(data, &result); decodeErr != nil {
			return decodeErr
		}
		if len(result.Violations) > 0 {
			messages := []string{"Floci compatibility check failed:"}
			for _, v := range result.Violations {
				messages = append(messages, fmt.Sprintf("%s -> %s: %s\n%s", v.Path, v.Operation, v.Reason, v.Fix))
			}
			return util.NewReadableError(nil, strings.Join(messages, "\n")+"\nNo AWS fallback was attempted.")
		}
	} else {
		return fmt.Errorf("Floci compatibility report missing; refusing to update: %w", readErr)
	}
	if err != nil {
		return err
	}
	identity := *p.floci
	if err := floci.EnsureIdentity(ctx, &identity, p.app.Name, p.app.Stage); err != nil {
		return err
	}

	a := p.loadedProviders["aws"].(*provider.AwsProvider)
	if _, err := a.Bootstrap(p.floci.Region); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Floci state: %s (instance %s)\n", p.floci.StateRoot, p.floci.Instance)
	err = p.runStack(ctx, input)
	if err != nil {
		return util.NewReadableError(err, fmt.Sprintf("Floci update stopped. Partial local state: %s. Fix the error and rerun, or use sst floci reset --stage %s to recover. No AWS fallback was attempted.", p.floci.StateRoot, p.app.Stage))
	}
	return nil
}
