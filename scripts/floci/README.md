# Floci compatibility spike

This is the first implementation step for #172. It is **not** `sst dev --floci` support. The only production change in this branch is the prerequisite fix that passes boolean and numeric provider settings to Pulumi. No SST command selects Floci yet.

The spike uses Floci **2.3.0**, pinned to Docker manifest digest `sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9`, and the AWS provider version in `platform/package.json` (**7.48.0** when tested).

## Run

Requirements: Docker, repository dependencies (`bun install`), Go, Node with `--use-env-proxy` support (tested with Node 26.7.0), and the repository's Pulumi CLI (3.267.0 when tested). The runner finds Pulumi in SST's config directory; set `SST_PULUMI_ROOT` to a directory containing `bin/pulumi` to use another installation. This overrides only the CLI binary: provider plugins must already be installed in SST's config directory (`~/Library/Application Support/sst` on macOS, `$XDG_CONFIG_HOME/sst` or `~/.config/sst` on Linux). The dead proxy intentionally prevents downloading plugins during the test.

From the repository root:

```sh
docker compose -f scripts/floci/compose.yaml up -d floci
curl --fail http://127.0.0.1:4566/_floci/init
SST_FLOCI_SPIKE=1 go test ./pkg/project/provider -run TestFloci -count=1 -v
node scripts/floci/spike.mjs
docker compose -f scripts/floci/compose.yaml run egress
```

Wait for `completed.ready: true` before running the tests. The Go bootstrap probe is intentionally restricted to `http://127.0.0.1:4566`. For the Pulumi/Compose probes, `FLOCI_SPIKE_PORT` changes both the published and container port; use the same value for both commands. State and results are under `.sst/floci-spike-<port>/`, isolated from SST stage state. Repeating the test changes the revision back to `one`, verifies stability, then updates it to `two` again.

The host HTTP callback listens on port 13579 while the test runs so Lambda containers can reach it. It is a connectivity probe, not the production SST bridge protocol. The test leaves the container, volume, resources and local state for inspection. `stop` preserves data. Only when deliberately cleaning up, run `node scripts/floci/spike.mjs destroy` for Pulumi resources; the bootstrap resources and Docker volume are separate and retained. No cleanup is automatic.

## Results (2026-10-30)

Verified on macOS arm64 with Docker 29.8.0:

- Existing SST Go bootstrap: S3 asset/state buckets, notifications, state bucket versioning, ECR repository, SSM persistence and reload through a new provider instance.
- HTTP `PutObject` succeeds after bootstrap installs the `aws:SecureTransport` deny policy, with Floci's default authentication settings. This does not establish behavior with stricter Floci auth settings.
- Pulumi creates an S3 bucket, DynamoDB table, SQS queue, IAM role/inline policy, Node.js Lambda, function URL/permission and SQS event source mapping.
- Second `up`: **11 unchanged**. Refresh followed by preview: **11 unchanged**.
- Update: tags on the bucket/table/queue and Lambda environment update successfully.
- Real Lambda execution reads `hello from S3` and `hello from Dynamo`, reports the updated revision, and reaches the host HTTP callback. The public function URL returns HTTP 200.
- Sending an SQS message invokes the subscriber and delivers its body to the host callback.
- Restarting the persistent Floci container preserves resources; the subsequent Pulumi run succeeds.
- Node's HTTPS proxy probe fails with `ECONNREFUSED`; Bun's standalone proxy probe also fails with `ConnectionRefused`. A Node process on an internal Docker network can reach Floci but cannot reach AWS STS over HTTPS.
- Repository platform build, CLI build, typecheck, CLI test suite and **291 platform tests** passed. Provider-config regression was observed failing before the fix, including a large number that would otherwise use scientific notation.

## Findings that change the draft

1. Readiness is `completed.ready`, not a top-level `ready`.
2. `FLOCI_BASE_URL=http://host.docker.internal:4566` produces function URLs ending in `.host.docker.internal`, which do not resolve on this host. Use `localhost.floci.io` for returned service URLs and give the Floci container that network alias. Reserve `host.docker.internal` for Lambda-to-host callbacks.
3. A mounted data volume alone does not persist service state: Floci defaults to memory storage. Set `FLOCI_STORAGE_MODE=persistent`.
4. Floci's Lambda Docker-flag parser accepts `--add-host host.docker.internal:host-gateway`, but rejects `--add-host=host.docker.internal:host-gateway`.
5. With multiple Docker networks, explicitly set the Lambda runtime API host override to the shared Floci hostname. Automatic network selection can choose an address outside the Lambda's network.
6. Node's proxy configuration must be applied at process startup. The spike relaunches with `--use-env-proxy` after scrubbing inherited AWS and proxy environment variables.

Configuration references: [Floci 2.3.0 Compose](https://github.com/floci-io/floci/blob/2.3.0/docker-compose.yml), [environment variables](https://github.com/floci-io/floci/blob/2.3.0/docs/configuration/environment-variables.md).

## Still required before shipping

- Production transport interface and actual SST bridge stream/resend tests. The HTTP callback proves connectivity only.
- SST components (rather than only raw Pulumi resources), component options, regional providers, custom resources and invokes; versioned compatibility profile and preview enforcement.
- Floci credential/endpoint configuration in every production Go/JS client; child-environment scrubbing; explicit dev-only flag and command rejection.
- Instance UUID, stage-state separation, reset/recovery and simultaneous-instance lifecycle tests. The two manually started instances in this session do not establish these guarantees.
- Full Pulumi/SST run in an egress-denied container, including negative unsupported-resource tests. The internal-network probe is deliberately narrower. Proxies do not sandbox arbitrary user code.
- Automated CRUD delete checks (not run because resources were requested to be retained), Linux testing, strict-auth policy checks, and cloud deployment regression testing. No real AWS deployment was performed.
- `sst floci` install/start/stop/status/reset commands, auto-start, documentation and CI integration. No supported compatibility matrix should be published from this spike alone.

During this session the initial exploratory container remains at port 4566 (`sst-floci-spike`, memory mode). The corrected persistent instance remains at port 4567 (`sst-floci-spike-verified`); its successful result is `.sst/floci-spike-4567/result.json`. These manually started containers are not owned by Compose. Use a fresh Docker environment for the Compose instructions, or explicitly reconcile the existing container names before running them. Neither instance is an SST stage.
