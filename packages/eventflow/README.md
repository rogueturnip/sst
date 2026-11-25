# @sst-community/eventflow

The runtime of SST's [`EventFlow`](https://sst-community.github.io/sst/docs/eventflow) component: event-driven durable functions, with an API shaped like Inngest's, on AWS Lambda durable functions.

```bash
npm install @sst-community/eventflow
```

```ts
import { EventFlow, serve } from "@sst-community/eventflow";

export const flow = new EventFlow({ id: "my-app" });

const welcome = flow.createFunction(
  { id: "send-welcome", triggers: { event: "user/created" } },
  async ({ event, step }) => {
    await step.sleep("wait-a-day", "1d");
    await step.run("send-email", () => sendEmail(event.data.email));
  },
);

export const handler = serve({ client: flow, functions: [welcome] });
```

## Versions

The component and the `sst eventflow` CLI are in SST. The runtime is here, with a version of its own, so a fix to the engine doesn't need an SST release: update this package and deploy.

They agree on a contract number, which the component puts in the functions' environment. A runtime that needs a newer component than the deployed one says so when it starts.

A run stays on the worker version it started on, so for a while a deploy has two versions of the runtime on one table. A release that changes what's in the table has to read what the version before it wrote.

## Develop

```bash
npm ci
npm test             # the engine, in the local durable test runner
npm run test:table   # the table's contract, on DynamoDB Local (Docker)
```
