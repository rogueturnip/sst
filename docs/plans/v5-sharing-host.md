# Sharing components, with SST as the host

**Status: done 2026-10-12, and on `v5`.** It was built on the branches `v5-sharing` and `v5-sharing-host`, and `v5` was moved forward to it, so each step below is a commit on `v5` and can be reverted alone. Planned in `2ecd2483c`.

## The goal

Three things together:

1. **Every app works, with a `package.json` or without one.** An app written in Python, Go or anything else has only `sst.config.ts`, and the CLI does the rest, as in 4.x.
2. **An app chooses its provider versions.** It sets them in `sst.config.ts`, as in 4.x, and is told when it picks one SST's components weren't written for.
3. **Components are shared as npm packages.** An app installs one and uses it like one of SST's. Its author writes, builds and tests it with ordinary tools, and needs no `sst.config.ts` to do so.

And, as for all of v5 (decided 2026-10-09, in [v5-components.md](v5-components.md)): an app's config is read the same way, and its state doesn't change.

## Decided (2026-10-12): SST is the host, and a component package is a plugin

- **The runtime stays SST's.** The CLI installs SST's platform, Pulumi and the providers in `.sst/platform`, as 4.x does. The app's package manager isn't involved, so it works the same with any of them, and with none.
- **A component package says what it needs.** It imports `sst/components` (or a group of it, like `sst/components/aws`) and the providers it uses, by their own names. It lists them in `peerDependencies`, for the apps that install it, and in `devDependencies`, for its own build and tests.
- **The deploy gives a package the app's copies.** The bundler maps `sst/components` to the platform, and a provider's import resolves from where the bundle is written, `.sst/platform`. So there's one copy of SST's components, of Pulumi and of each provider because of how the deploy is built, not because a check found no second one. `v5-sharing` already does this.
- **The `sst` npm package carries SST's components, for authors.** Compiled to JavaScript, with their types, their groups and `sst/testing`. A package's repo installs it and the providers it uses as ordinary dev dependencies. An app's deploy doesn't use it.

It's the pattern an editor's extensions follow: they get the editor's API from the editor they run in, not from their own dependencies.

### Why not providers in the app's package.json

[That proposal](v5-providers-in-package-json.md) was tried on the local branch `v5-sharing-deps`, in sixteen steps. The branch was deleted on 2026-10-12. With npm and pnpm, an app moved over without its state changing. But it moved a problem only a package's repo has onto every app:

- **An app without a `package.json` needed one**, with Pulumi and its providers in `node_modules`.
- **SST no longer chose the versions.** Yarn needs a `resolutions` pin, and more than one copy of `@pulumi/pulumi` is possible with any package manager. A check on deploy catches them, but can't prevent them.
- **The app's package manager was in the path.** Yarn Plug'n'Play wasn't supported.
- **The CLI and the `sst` package had to be the same version**, and Node had to be 20.6 or newer.
- **`sst install`, `sst add`, `sst upgrade` and the run were rewritten.** They're SST's code, so every merge of an SST release would conflict there.
- **It was about 2,900 lines over 22 commits**, and never deployed to AWS.

Some of its steps are worth having either way. The steps below bring them over.

## Where v5-sharing leaves it

[Sharing components as packages](v5-sharing.md) works under the mock. What's still missing for the goal:

- **A package's repo needs an `sst.config.ts`**, only so `sst install` fetches SST and the providers to compile against. Then it extends `.sst/tsconfig.json`, and its vitest config uses an `alias`.
- **An app without a `package.json` can't install a package.**
- **A provider version in the config isn't checked.** Any version is installed, another major version included.
- **An error in a file given the globals is reported on the wrong line,** since their imports are added at the top of the file.
- **Nothing has been deployed to AWS**, and `sst dev` hasn't been run with a function inside a package's component.

## What it looks like

### An app

As in 4.x. A version in the config replaces the one SST comes with. An app with a `package.json` installs a package as a dev dependency (`npm install -D @acme/email`). An app without one lists it in the config, and `sst install` installs it in `.sst/platform` (step 7):

```ts title="sst.config.ts"
export default $config({
  app(input) {
    return {
      name: "my-app",
      home: "aws",
      providers: { aws: { version: "7.50.0" } },
      packages: { "@acme/email": "1.2.0" },
    };
  },
  async run() {
    const { Email } = await import("@acme/email");
    new Email("Welcome", { sender: "hello@example.com" });
  },
});
```

It's `packages` in `app()`, beside `providers` (step 7).

### A component package

```json title="package.json"
{
  "name": "@acme/email",
  "type": "module",
  "exports": {
    ".": { "types": "./dist/index.d.ts", "default": "./dist/index.js" }
  },
  "peerDependencies": {
    "sst": "^5.0.0",
    "@pulumi/aws": "^7.48.0"
  },
  "devDependencies": {
    "sst": "npm:@sst-community/sst@5.0.0",
    "@pulumi/pulumi": "3.267.0",
    "@pulumi/aws": "7.48.0"
  }
}
```

```ts title="src/index.ts"
import { component } from "sst/components";
import * as aws from "@pulumi/aws";
```

```ts title="test/email.test.ts"
import { mock } from "sst/testing";
```

There's no `sst.config.ts`, no `sst install` and no `.sst`. `tsc` builds it, and vitest, `node --test` and `bun test` run its tests. On `v5-sharing-deps`, a package's repo with only `sst`, Pulumi and the aws group's providers installed passed all four.

### Who sees what change

| Who | On `v5-sharing` | After this plan |
| --- | --- | --- |
| An app with a `package.json` | Installs a package with its package manager | The same |
| An app without one | Can't use a package | Lists it in the config |
| An app that sets a provider version | Any version is installed | A version in another major version than SST's is refused |
| A package's repo | `sst.config.ts`, `sst install`, `.sst/tsconfig.json` and a vitest `alias` | Ordinary dev dependencies |
| A deploy | One copy of SST's components and of each provider, from `.sst/platform` | The same |

## Steps

Each is its own commit. A step taken from `v5-sharing-deps` names its commit there, and its step in that branch's copy of the providers note.

1. **The rule for 4.x component files** (done, `6eca18d4e`). Bring over `08f94b94d`: a 4.x component file can change where it changes nothing for an app (the config reads the same, and state doesn't change), and V5 components can change in any way. It was decided on 2026-10-09 for all of v5, but only committed on `v5-sharing-deps`, so the rulebook here still says not to edit a 4.x file. Steps 3 and 4 rely on it. Leave out its line in the providers note. It could go to `v5` directly instead.

2. **An error's line in an app file** (done, `e37a04b30`). Bring over `cad5baa86` (step 13): the globals' imports go at the end of a file instead of the top, so its lines keep their numbers. It's a few lines in `pkg/js/js.go`, likely redone by hand, since that file moved on there before it. Checked by its test, which runs a bundle with source maps and checks the line.

3. **SST's types name no globals** (done, `24de2a1ee`). Bring over `ae3d39f0a` (step 14). The `sst` and `$util` aliases move to `platform/src/global-aliases.d.ts`, which the types build leaves out, and the 19 uses of `$util` in types become imports from Pulumi. Without it, SST's declarations name types through its globals, 487 times. A package's repo has no globals, so there `tsc` fails on a V5 component's `nodes`, and Pulumi's types become `any`. Leave out its line in `dependencies_test.go`, which isn't on this branch. The templates' `global.d.ts` imports the aliases too, which that commit missed. Checked: `tsc`, `declarations.test.ts`, and the generated docs identical before and after.

4. **A core, and a group for each provider** (done, `5471a11ad`). Port the components part of `7530dfa56` (step 11):
   - `sst/components` is the core: what SST's components share. `sst/components/aws`, `/cloudflare` and `/vercel` are each provider's components. The `sst` global is all of them, as before.
   - `PlatformModules` lists the four, so a deploy maps each to the platform. `.sst/tsconfig.json` and the vitest `alias` map them too, which that commit didn't need, and a test checks the alias against the groups.
   - `mock()` gives the `sst` global the groups whose providers are installed, so a package's test needs only its own providers.
   - The Cloudflare API token is read when it's used, not when `cloudflare/helpers/fetch.ts` loads. That's a 4.x file (step 1).

   Leave out the removal of step 10's stand-ins (not on this branch), its `sdk/js` changes (step 5 here), and its tests of the install from the app's `package.json`. A package that imported a group's component from `sst/components` now imports it from the group. That's unreleased. Checked: `groups.test.ts`, `global.test.ts`, and that the deploy's package check finds a package that imports only a group. And with CLIs built before and after it, on an app with local state, random and tls, a file using the globals and a package importing `sst/components`: deployed with the one from before, `sst diff` with the one after showed no changes, and its deploy changed only the encrypted values that a second deploy changes too. A package's `sst/components/aws` and `sst/components` gave it the `sst` global's `Bucket` and `Linkable`, and that app's `.sst/tsconfig.json` gave TypeScript the groups' types. A package's `.d.ts` names a group's component through the group (`declarations.test.ts`), and reading `app()` leaves the groups out, as it does `sst/components` (`TestBuildReadingApp`).

5. **The `sst` package carries SST's components** (done, `d3522d217`). Port the package parts of `e5b14cdf0` (step 8) and `49ac2c834` (step 15):
   - `sdk/js/scripts/platform.ts` compiles the platform to JavaScript, with its declarations, into the package's `platform` folder. Its entries are what the package exports: the core, each group and `sst/testing`. The `sst` global's `index.ts`, `auto/run.ts` and `shim/run.js` are left out, since only a deploy loads them.
   - The package exports `sst/components`, its three groups and `sst/testing`.
   - The release adds what the platform loads as it runs (esbuild, archiver and glob) to the published `package.json`, and lists each group's providers.

   Leave out step 8's CLI changes. An app's deploy still builds from `.sst/platform`, so nothing requires the package and the CLI to be the same version. Checked: the pack loads every export in plain Node, the groups included, and stops when one fails to load (tried by breaking one). Packed, it's 1.2 MB, 6.4 MB unpacked, and adds esbuild, archiver and glob to the SDK's dependencies.

6. **A package's repo with ordinary tools** (done, `6bd64acd0`). With the packed `sst` and no `sst.config.ts`, `tsc` emits the package's declarations, and vitest, `node --test` and `bun test` pass. Then the docs: "Share it" in the component guide and the rulebook stop telling an author to run `sst install` and extend `.sst/tsconfig.json`. An app keeps both, and its tests keep the `alias`, since its providers are in `.sst/platform`.

   Checked, in a repo with the packed `sst`, Pulumi and the aws group's providers installed from npm, and nothing else of SST's:
   - One copy of `@pulumi/pulumi`. `tsc` emitted a `.d.ts` that names only `sst/components`, `sst/components/aws` and Pulumi, with the component's outputs typed.
   - A component with a V5 `Bucket` from `sst/components/aws` as a part, and a role for each team: vitest 5 and `bun test` ran it from the source, and `node --test` from what `tsc` built. Each recorded the bucket, its four resources and the roles, named for the app and stage, and the mock's `sst.aws.Bucket` was the one the package imports.
   - The guide's example, the component and its test as written, passed with `@pulumi/cloudflare` added.
   - Without `@pulumi/command`, one of the aws group's providers, importing `sst/components/aws` failed with Node's own `Cannot find package '@pulumi/command'`. The guide says to install a group's providers, and where they're listed.

   The message `mock()` gives for a group whose providers aren't installed said "the app hasn't installed" them. It now says what to do in a package's repo, and in an app. `MAINTAINING.md` says how to pack `sst` for a dev build.

7. **Packages listed in the config,** for an app without a `package.json` (done, `58ea65783`, and bun 1.4.2 in `054e776df`):
   - `packages` in `app()`, beside `providers`, each with a version (decided 2026-10-12).
   - `sst install` adds them to `.sst/platform/package.json`, and installs them with the providers.
   - The bundler already falls back to `.sst/platform/node_modules` (`NodePaths` in `pkg/js/js.go`), so the config's import resolves.
   - `.sst/tsconfig.json` maps each one, so the editor finds its types.
   - The deploy's check of a package's `peerDependencies` covers them, as it covers a package in `node_modules`.
   - A package in both the app's `package.json` and the config is refused, so there's never two copies.

   What it took besides:
   - **Reading `app()` before the first install.** The CLI reads `app()` before `sst install` runs, and a listed package isn't installed yet. That build already left out the providers. Now it leaves out any package it can't find, with a stub that says it's only loaded in `run()`. A build that runs the app still fails on a package it can't find.
   - **What's installed is recorded after the install succeeds,** in `.sst/platform/installed-packages.json`, as the provider lock is. The platform's `package.json` couldn't say: it's written before bun runs. A package the config no longer lists is taken out of the platform, its folder too, which bun leaves.
   - **The config is checked when it's read:** a version is required, and a package can't be a provider, `@pulumi/pulumi`, one of SST's own dependencies, or in the app's `package.json` too. Each says what to do instead.
   - The config's type documents `packages`, and the guide's "Use it" shows it.

   Checked: Go tests for the check, the install, the record, the removal, the tsconfig paths, and reading `app()` before the install (which fails without the stub). And with the CLI, on an app with only `sst.config.ts`, local state and a package from a tarball: the first deploy installed the package and created its resources. Another diff installed nothing and found no changes. TypeScript read the package's types through `.sst/tsconfig.json`. The same package in a `package.json` was refused. A version whose peer dependencies the app can't meet was stopped by the deploy's check, with bun 1.4.2. Going back installed the first version. Taking the package out of the config took it out of the platform.

   **Found while building it: SST's bun hung.** bun 1.2.1, which SST pinned, ran at full CPU without end installing a package from a `file:` tarball in `.sst/platform`. It did every time the package's peer dependencies conflicted with what the platform installs, here `@pulumi/random` `^5` against 4.21.2, and once without a conflict. A command just stops there, before the deploy's check could say what's wrong. On its own, with only the two packages, bun 1.2.1 hung too, with `--omit peer` as well. bun 1.4.2 installs the same in a second and warns about the peer. npm stops with `ERESOLVE`. A provider with a conflicting peer can hit the same, on 4.x too. So SST pins bun 1.4.2 now, released 2026-09-05. SST runs bun only to install `.sst/platform`. Checked, on an app with local state, random, tls, a file using the globals and a package: deployed with bun 1.2.1, the CLI with 1.4.2 installed the platform again and `sst diff` showed no changes. Every package both installed was at the same version, the providers included. Only where a few copies of `@types/node` are nested differed. The lockfile stayed `bun.lockb`.

8. **A provider version in another major version is refused** (done, `fd29a89f4`). The rule is from `64fe7d481`: SST's components are written for one major version of each provider they use (before 1.0, one minor version).
   - `sst install` refuses a version in the config outside it, after 4.x's check for an older one. It says which to use: "You specified version 5.4.1 of the "vercel" provider. SST's components are built for 4.x, and a new major version can remove what they use. Use a 4.x version, 4.6.0 or higher."
   - A provider SST's components don't use isn't checked, and neither is one the config gives its own `package`.

   What it took besides: **`sst add` picked a version the install would refuse.** It wrote the version npm tags latest into the config, and for vercel that's 5.4.1, a major version past SST's 4.6.0. For a provider SST's components use, it now picks the newest version in their major version, here 4.6.1. Prereleases aren't picked. Where latest is already in it, as for aws, it's what `sst add` picked before. The CLI reference and the providers page say so, and their examples use an aws 7 version, since SST already refused the 6 they showed.

   Checked: Go tests, with a stand-in registry, for the rule, the install and `sst add`, and the message. And with the CLI, on an app with only `sst.config.ts`: `vercel: "5.4.1"` stopped at `sst install` with the message above. 4.6.1 installed. 4.5.0 was still refused by 4.x's check. With no providers, `sst add vercel` wrote 4.6.1 into the config and installed it.

9. **Run it** (done, `cbe97c02b`, and the fix in `f099af401`).
   - **Locally, with state in a local file.** Three apps were deployed with a CLI built from `v5-sharing`, before any step here, and diffed with one built from the last step: an app with only `sst.config.ts`, and an npm and a pnpm app, each using a package from `node_modules` (pnpm's is a link into its store). None showed changes. The first CLI installed `.sst/platform` with bun 1.2.1, the second with 1.4.2. Then the app with only `sst.config.ts` listed the package under `packages`: the deploy created its resources, and another diff found no changes.
   - **In AWS, on a stage of the lab app of its own.** A package whose component makes a V5 `Function` from a handler in the package. It finds the handler with `createRequire(import.meta.url).resolve("@acme/greeter/handler")`, so it needs no globals, and that works wherever the package is installed.
     - With the package listed in the config, the deploy created the function, and it answered from Lambda.
     - Moved to the app's `package.json`, `sst diff` showed only the function's code changing. esbuild writes the handler's path into the bundle as a comment, and that path changed.
     - `sst dev`, with the package in each place: a call to the function was answered from the machine, and the CLI built the handler from the package's folder, in `node_modules` and in `.sst/platform/node_modules`.
     - Removed. The stage's passphrase, sourcemap and state files were deleted after, as SST leaves them. The lab's audit lists the same as before the run.

   **Found: the CLI panicked after deploying a component whose type has two parts,** like `acme:Greeter`, as the guide writes them. Reading the deploy's results, it asked Pulumi for the type's module, and Pulumi fails when there's none. Everything was created and the state was saved by then. But the update wasn't marked finished, and `outputs.json` wasn't written. The CLI now takes the type's first part as its package, which is the same for a type with three. The code reads every resource in the state, so an app's own component written as in the guide hits it too. So would a 4.x app with a Pulumi `ComponentResource` of such a type: `main` and SST have the same line. What came of it is under "Decided along the way".

10. **Record it** (done). The status here, the log in [v5-progress.md](v5-progress.md), the decisions in [v5-components.md](v5-components.md), and [v5-sharing.md](v5-sharing.md). `v5-sharing-deps` can be deleted.

Steps 1 and 2 stand alone, and could go to `v5` first. Steps 3 and 4 need step 1. Step 5 needs steps 3 and 4, and step 6 needs step 5. Steps 7 and 8 need none of the others.

## Decided along the way

- **The config field for packages** (2026-10-12): `packages` in `app()`, beside `providers`, with a version required (step 7).
- **An older provider version, within SST's major version:** already refused. 4.x stops with "SST needs 7.48.0 or higher" (`ErrProviderVersionTooLow`), so step 8 added only the upper bound.
- **A component's type has three parts** (2026-10-12, `38447dc77`), `<package>:<module>:<Name>`, as Pulumi's convention is and as SST's own components have. `component()` refuses a type with two parts and says what to write, and the guide's and rulebook's examples use `acme:index:Uploads`. A link's type follows, as `acme.index.Uploads`. The CLI's fix for two parts (step 9) stays on v5 for now, not on `main` or sent to SST.
- **A function's code in a package** (2026-10-12, `70b321483`): the guide says how, in "Ship a function's code". The handler is in the package's `exports`, and the component finds it with `createRequire(import.meta.url).resolve()`. There's no helper for it.
- **A provider a package needs that the app hasn't added** (2026-10-12): the deploy keeps saying to add it with `sst add`. A provider's settings, like its region, are in the config's `providers`, so that change is the app's to make.
- **The platform's runtime dependencies in production installs** (2026-10-12): they stay in the one `sst` package. An app that lists `sst` in `dependencies`, for `sst/resource`, also gets esbuild, archiver and glob. Every install of `sst` already brings the CLI's binary, about 96 MB, so they add little, and a second package would have to be released with every version.

## Known limits

- **Pulumi's version is the CLI's.** The CLI runs Pulumi's engine at that version, so an app can't choose it, as in 4.x.
- **A provider's next major version waits for a release of SST.** An app can't take one for a provider SST's components use, vercel 5 for example, even if it doesn't use those components. 4.x installed it, so an app that has one in its config is refused when it moves to v5, and has to go back to SST's major version.
- **Renovate and Dependabot don't see the versions in `sst.config.ts`.** A custom manager in Renovate could.
- **A JavaScript app has SST's components twice on disk:** in `.sst/platform`, which the deploy uses, and in `node_modules/sst`, which nothing in the app uses. They're the same version when the CLI hands off to the app's `sst`.
- **The CLI hands off to the app's `sst` only from `./node_modules/.bin/sst`.** In a monorepo where `sst` is installed at the root, the CLI on the path runs instead, as in 4.x.
- **The limits of `.sst/tsconfig.json`** in [v5-sharing.md](v5-sharing.md) still apply to apps.
- **A package listed in the config gets no globals.** The files in `.sst` aren't given them, and that's where it's installed. A package that imports what it uses, as the guide says to, doesn't need them.
- **A package's repo needs `skipLibCheck`.** Without it, `tsc` finds 45 errors in SST's declarations: types from providers the repo doesn't install, like cloudflare's from `sst/components/aws`, and a `T` that `cloudflare/ai.d.ts` doesn't declare.
- **A package that uses a group installs all of that group's providers,** `@pulumi/command` and the rest for `sst/components/aws`, though it only uses aws. Without one, the import fails with Node's own message, not SST's.
- **`sst` 5.x is only out as pre-releases** until 5.0.0, under npm's `next-experimental` tag (since 2026-10-13). A package's peer dependency on it is written `>=5.0.0-0`, so that it includes them.

## What comes from v5-sharing-deps

That branch was deleted on 2026-10-12, so its commits named here can't be looked up any more. The table says what each one was.

| Commit there | Its step | Here |
| --- | --- | --- |
| `08f94b94d` | The rule for 4.x files | Step 1, as it is |
| `cad5baa86` | 13 | Step 2 |
| `ae3d39f0a` | 14 | Step 3 |
| `7530dfa56` | 11 | Step 4, the components part |
| `e5b14cdf0`, `49ac2c834` | 8, 15 | Step 5, the package parts |
| `64fe7d481` | Newer providers within a major version | Step 8, the rule only |
| The rest | 1 to 7, 9, 10, 12, 16 | Not brought over. They move the providers into the app's `package.json`, or fix what that caused. |
