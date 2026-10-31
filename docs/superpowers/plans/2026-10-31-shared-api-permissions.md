# Shared API invocation permissions implementation plan

> Execute inline using superpowers:executing-plans, with a fresh whole-branch review.

Goal: solve #215 without changing default route permissions.
Spec: https://github.com/sst-community/sst/issues/215
Architecture: an explicitly named `api.addHandler(name, arn)` binding owns one permission under the API. Routes reuse the binding's ARN and permission. Register resources eagerly; ARN/qualifier values may be unknown. No global cache or permission resources inside apply callbacks.

## Constraints and review focus

- Branch from main, no Floci changes or new dependencies.
- Each binding belongs to one API; reject reuse by another API.
- Qualified ARNs must use the exact alias/version in both permission and integration.
- Stable permission name/parent must not depend on route order or count.
- Default routes keep their permission URNs; shared integrations depend on the shared grant.
- Document two-step migration and retained grants until the binding is removed.

## Tasks

- [x] Add component tests across HTTP, REST and WebSocket for shared grants, qualifiers, defaults, API separation and unknown-ARN previews. Run and observe missing-method failure.
- [x] Add the internal binding helper and public addHandler methods. Thread binding into each Lambda route without altering ordinary handlers. Run focused tests and typecheck.
- [x] Add an example and migration documentation in JSDoc. Run full tests/builds and deploy the example, verify policy count and invocation; retain resources.
- [ ] Review the branch, fix important findings, commit and open a PR with Closes #215.

Verification: 309 platform tests, Go suite, typecheck, platform/CLI builds passed. AWS example: 123 Lambda routes across three APIs, three invocation grants; HTTP/REST requests and WebSocket handshake passed. HTTP/REST calls through sst dev passed. Independent review found no blocking issues.
