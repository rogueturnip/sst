# Policies made easier to use: an experiment

Tried on 2026-10-15, on a local branch, with apps on local state and a plain Pulumi project. Nothing ran against an AWS account. **Not acted on**: the notes are kept for later, and nothing here is planned yet.

The prototype is a local branch, `spike/policies`, which isn't pushed. It's one commit on `v5` at `c14e5a752`, with its tests and the rules package. 8 tests that read `mock()`'s old policy document shape fail on it.

## Why

The one answer on [Discussion #59](https://github.com/sst-community/sst/discussions/59) so far (jl4nz):

- They enforce rules with their own Pulumi policies, through SST's `--policy`.
- Asked whether a component should lock settings so `transform` can't change them: no, "a policy should catch before deployment".
- The gap is a maintained set of rules, as "a separated project, like CDK-NAG". Pulumi's own AWS policies are out of date.
- Shared packages raise a question of trust: a package can be compromised or go stale.

So the experiment kept policies out of components: no locks, and nothing a component author writes. A component package can't lock anything anyway. The app runs its code, and `$transform(aws.s3.Bucket, ...)` changes every bucket after the component has created it.

## What was built

1. **`policies()` in `$config`**, next to `app()` and `run()`. The CLI builds it into a policy pack in `.sst/platform/policy`, from the same build as the program, and passes it to `pulumi preview` and `pulumi up` on `diff`, `deploy` and the deploys of `sst dev`. The rules use the app's providers and SST's `@pulumi/policy`, so there's no second project to install or keep in step.
2. **`sst.rule(name, ResourceClass, check, { level, description })`.** `check` gets the resource's inputs, typed, and a `ctx` with the stage, the owner (the component and the part) and `fail(message, { set })`. With `set`, the message says which `transform` fixes it: `Secrets › password: is 8 characters. Use at least 20. Fix: transform: { password: { length: 20 } } on "Secrets".`
3. **`sst.exception(policy, resource, reason)`.** The resource is named as its component and part, or by its own name. A violation it covers is reported as accepted, with the reason, and never stops a deploy. An exception to a policy that isn't there is refused.
4. **Plain Pulumi policies mix in.** `policies()` returns `sst.rule`s and Pulumi policies in one list. Every message names the component and part, Pulumi policies' too. A classic pack given with `--policy` runs next to the generated one.
5. **A deploy previews first only when a policy can stop it.** Before Pulumi runs, the CLI asks the pack which of its policies are mandatory for the stage, which takes about 0.4s. A pack given with `--policy` can't be asked, so it always previews first.
6. **`app.check(policies)` under `mock()`.** It runs `sst.rule`s and Pulumi policies against what a test created, with the arguments Pulumi gives them.
7. **A rules package, as CDK-NAG is.** 7 AWS rules as plain Pulumi policies that import nothing: Lambda runtime, log retention, SQS encryption, DynamoDB point-in-time recovery, RDS encryption, IAM admin wildcards, S3 public access. The package also has a classic pack entry. Apps installed it from a git tag.

## What it showed

| | Result |
| --- | --- |
| No second project | Rules in the config, built with it. The config loads a second time in the policy process, and the globals (`sst`, the providers, `$app`) work there. |
| Final inputs | A `$transform` outside the component that lowered a password was caught. |
| Strictness by stage | `$app.stage` in the config: a warning on `dev`, blocked on `production`. |
| A blocked deploy | With the preview first, it left only the stage's passphrase. |
| `sst dev` | The policies ran on each deploy, and the warnings showed once the dev UI was told about them. |
| Tests | 6 tests for rules, exceptions and naming, and 3 for the rules package against the v5 `Bucket`, `Function`, `Queue` and `Dynamo`, all under `mock()`. |
| Rules from git | Installed by tag, settings passed in (`minLength: 12`), the providers still SST's one copy. |
| The rules package outside SST | As a classic pack in a plain Pulumi project, it caught all five expected violations and passed a properly blocked bucket. |
| Types | `policies()`, `sst.rule` and the Pulumi policy types resolved in an app. A deliberate mistake was caught: `password.length` is `number \| undefined`. |
| SST's own defaults | They pass the baseline: tables have point-in-time recovery, logs keep a month, functions use `nodejs24.x`, buckets block public access. Only a bucket made public on purpose was flagged. |

Time, on an app of a few resources:

- `sst diff`: 2.2s without policies, 3.2s with them, to start the policy process.
- A deploy with nothing to change, where policies only warn: 3.6s. It keeps Pulumi's `--skip-preview`.
- The same, where a policy can block: 6.2s. The program runs twice, for the preview and for the update. On a large app this is the real cost, and it can't be avoided if a blocked deploy is to change nothing.

## Bugs it found in SST today

These are there without the design, and each can be fixed on its own.

1. **`sst deploy --policy` can deploy part of an app before a mandatory policy stops it.** SST runs `pulumi up` with `--skip-preview`, so policies are checked during the update, after what came before is deployed. Shown with a classic pack and today's CLI: a component and its password were deployed, then the deploy failed. The fix is to preview first when a policy pack is given.
2. **`sst dev` never shows policy warnings.** The dev UI isn't subscribed to `PolicyAdvisoryEvent` (`cmd/sst/ui.go`). The policies run; their warnings are dropped. The docs say policy packs work with `sst dev`.
3. **`mock()` can't run `$transform` on a provider's resource class**, like `aws.s3.Bucket`, because it has no root stack resource. Creating one with `runInPulumiStack` fixed it, and the platform's tests still passed.
4. **`mock()` answers `aws.iam.getPolicyDocument` with its inputs**, not the IAM JSON AWS's provider makes. A test can't read a policy the way a deploy does. Rendering the JSON fixed that and broke 8 tests that read the old shape.

Also seen: the CLI reads its bootstrap from SSM whenever an app lists the `aws` provider, even with `home: "local"`. So such an app can't be previewed without AWS.

## What was learned

- **Pulumi's policy runner** finds the pack's entry through its `package.json` (`main`, and a `version`), not `PulumiPolicy.yaml`. It loads the entry with `require()`, so the entry can't use top-level `await`.
- **A value that's only known after the deploy throws when a policy reads it in a preview.** In a stack check, Pulumi then drops the whole check, with no warning. SST wraps every policy in `policies()`, so it could catch that and say "can't check this until it's deployed" instead.
- **A rule needs testing both under `mock()` and in a real preview.** Each found a bug in the rules package the other didn't:
  - the unknown value above;
  - linking a public access block to any bucket with the same parent, which for top-level resources is the stack.
- Policy names have to be unique within a pack. Pulumi puts the description first in a message, and a policy without one repeats its name there. During the preview step of a deploy, the UI says "Created" for resources it hasn't created.

## What it would cost

- New API on `v5`: `policies()`, `sst.rule`, `sst.exception` and `app.check()`. About 440 lines in the prototype, tests included, plus docs.
- Two ways to write a rule. `sst.rule` is shorter and gives fix hints, but only works in SST. A plain Pulumi policy works anywhere.
- A rules project is ongoing work. Its lists go out of date, as Pulumi's did, and each rule needs both kinds of test. That rests on one person's answer so far.
- It's still not a lock. The app's owner controls the config, so an organization enforces in CI, with a deploy role only CI can use, and in AWS itself.

## If it's picked up

1. **Fix the four bugs above.** They're small, and the first two affect anyone using `--policy` now.
2. **`policies()`, `sst.rule` and exceptions on `v5`.** Add the "can't check until deployed" message, and fix the preview's "Created" labels.
3. **A rules project, only after asking.** Ask jl4nz and the Discussion which rules they keep and which of Pulumi's are out of date. Start with a few rules for what SST's components create.
