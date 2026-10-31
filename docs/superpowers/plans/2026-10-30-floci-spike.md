# Floci foundation and compatibility spike

> Implementation: inline using the executing-plans skill. This is the first delivery in the supplied Floci design, not the complete dev backend.

**Goal:** Establish repeatable evidence for Floci 2.3.0 and fix the scalar provider-config prerequisite.

**Architecture:** Keep the spike opt-in and separate from CLI target selection. Run SST's existing Go bootstrap and a raw Pulumi AWS program against a digest-pinned local Docker instance, with static credentials and local state. Preserve cloud behavior.

**Spec:** Supplied `floci-plan.md`, decisions dated 2026-10-29. Full dev support, compatibility enforcement, state identity/reset, CLI lifecycle commands, and the production live transport remain follow-on work after the spike.

## Tasks

- [x] Branch from fetched `origin/main`; pin the Floci image by digest.
- [x] Extract provider config argument construction without changing behavior. Add a JSON-input regression for true, false, zero, fractional numbers, strings, maps, arrays, null and SST-only package metadata. Observe failure, add scalar support, rerun project tests.
- [x] Add a repeatable Docker/Pulumi spike and opt-in Go bootstrap integration test. Exercise create, refresh, second update, updates, resource reads, function URL and queue delivery. Keep local resources/state for inspection; removal is explicit.
- [x] Probe container-to-host transport and endpoint isolation. Record actual results and limitations, including unsupported operations, rather than declaring a compatibility profile prematurely.
- [x] Run platform build, typecheck, CLI and platform tests; review the diff and document any failures.

## Review focus

- False and zero must reach Pulumi config; null and package metadata must not.
- Missing endpoint configuration must never cause the spike to use ambient AWS credentials.
- Refresh and an empty second update must expose emulator/provider drift.
- Lambda containers must reach both the emulator and a host transport.
- Bootstrap HTTP requests must reveal whether the SecureTransport deny policy is enforced.

## Decisions

- Use the existing clean checkout on a new branch; leave the previous branch intact.
- Use a named spike container/volume and local Pulumi backend, separate from any eventual managed `sst-floci` instance.
- No GitHub posts or PR in this phase; findings stay in the repository for review.

## Verification and review record

- Scalar regressions failed before their fixes (dropped booleans/numbers; scientific notation for 1000000), then passed.
- Live bootstrap and both complete Pulumi runs passed; final run used CLI 3.267.0, AWS provider 7.48.0 and corrected persistent Floci 2.3.0 configuration. See `scripts/floci/README.md` for findings and retained resources.
- Final full Go test suite passed. Project/provider race tests passed. Platform build, CLI build, typecheck and 291 platform tests passed.
- Independent review found explicit teardown would encounter the probe's nonempty bucket. Added `forceDestroy` to the test bucket and applied it in the final passing update. Actual deletion remains untested to preserve resources.
- Clarified that overriding the Pulumi binary does not override its plugin directory.
