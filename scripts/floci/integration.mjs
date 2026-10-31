import assert from "node:assert/strict";
import { request as httpRequest } from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import {
  readFile,
  writeFile,
  mkdir,
  cp,
  appendFile,
  readdir,
  rm,
} from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";

const cli = process.env.SST_TEST_CLI || "/usr/local/bin/sst";
const endpoint = process.env.SST_FLOCI_ENDPOINT || "http://127.0.0.1:4566";
const app = "/work/app";
const negative = "/work/negative";
const stage = "floci-ci";
function command(program, args, cwd = app) {
  const result = spawnSync(program, args, {
    cwd,
    env: process.env,
    stdio: "inherit",
    timeout: 300_000,
  });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${program} ${args.join(" ")} failed`);
}
const negativeRuns = {
  resource: 'new aws.ec2.Vpc("Unsupported", {cidrBlock: "10.0.0.0/16"});',
  component: 'new sst.aws.SnsTopic("Unsupported");',
  option:
    'new sst.aws.Function("Unsupported", {handler: "api.handler", vpc: {securityGroups: [], privateSubnets: []}});',
  invoke: "await aws.ec2.getVpcs({});",
};
function negativeConfig(body) {
  return `export default $config({app(){return {name:"floci-negative",home:"aws",removal:"remove",providers:{aws:{version:"7.48.0"}}}},async run(){new sst.aws.Bucket("MustNotBeCreated");${body}}});\n`;
}
// Syntax-check every generated fixture before preparing any external resources.
for (const body of Object.values(negativeRuns))
  new Function(negativeConfig(body).replace("export default ", "return "));
if (process.argv[2] === "prepare") {
  const pkg = JSON.parse(await readFile(`${app}/package.json`, "utf8"));
  pkg.dependencies.sst = "file:/repo/sdk/js";
  await writeFile(`${app}/package.json`, JSON.stringify(pkg));
  await mkdir(negative, { recursive: true });
  await cp(app, negative, { recursive: true });
  await writeFile(
    `${negative}/sst.config.ts`,
    negativeConfig(negativeRuns.resource),
  );
  for (const cwd of [app, negative]) {
    command("bun", ["install"], cwd);
    command(cli, ["install", "--stage", stage], cwd);
  }
  const deps = JSON.parse(
    await readFile("/repo/platform/package.json", "utf8"),
  ).dependencies;
  for (const provider of ["aws", "random"]) {
    command("/work/config/sst/bin/pulumi", [
      "plugin",
      "install",
      "resource",
      provider,
      deps[`@pulumi/${provider}`],
    ]);
  }
  process.exit(0);
}
assert.equal(process.argv[2], "verify", "use prepare or verify");
// Bound SDK/socket stalls too, not only readiness polling and child shutdown.
setTimeout(() => {
  console.error("Floci integration exceeded its 12 minute deadline");
  process.exit(1);
}, 12 * 60_000).unref();

async function until(label, test, timeout = 120_000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      const result = await test();
      if (result) return result;
    } catch (error) {
      last = error;
    }
    await sleep(500);
  }
  throw new Error(`${label} timed out${last ? `: ${last.message}` : ""}`);
}
async function request(url, options = {}) {
  if (options.headers?.Host) {
    // fetch derives Host from the URL; Node HTTP preserves this explicit virtual host.
    return new Promise((resolve, reject) => {
      const req = httpRequest(
        url,
        { ...options, timeout: 20_000 },
        (response) => {
          const chunks = [];
          response.on("data", (chunk) => chunks.push(chunk));
          response.on("error", reject);
          response.on("end", () =>
            resolve(
              new Response(Buffer.concat(chunks), {
                status: response.statusCode,
              }),
            ),
          );
        },
      );
      req.on("error", reject);
      req.on("timeout", () =>
        req.destroy(new Error("function URL request timed out")),
      );
      req.end();
    });
  }
  return fetch(url, { ...options, signal: AbortSignal.timeout(20_000) });
}
await until("Floci readiness", async () => {
  const response = await request(`${endpoint}/_floci/init`);
  return response.ok && (await response.json()).completed?.ready;
});
assert.equal(
  (await (await request(`${endpoint}/_floci/health`)).json()).version,
  "2.3.0",
);
// No proxy is installed in this test process. This must fail at the Docker network.
await assert.rejects(
  fetch("https://sts.us-east-1.amazonaws.com", {
    signal: AbortSignal.timeout(5000),
  }),
  "isolated network allowed public AWS access",
);

const require = createRequire(`${app}/.sst/platform/package.json`);
const {
  S3Client,
  ListBucketsCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
const {
  SSMClient,
  DescribeParametersCommand,
  GetParameterCommand,
} = require("@aws-sdk/client-ssm");
const {
  LambdaClient,
  ListFunctionsCommand,
} = require("@aws-sdk/client-lambda");
const appRequire = createRequire(`${app}/package.json`);
const { DynamoDBClient, ListTablesCommand, GetItemCommand } = appRequire(
  "@aws-sdk/client-dynamodb",
);
const { SQSClient, ListQueuesCommand } = appRequire("@aws-sdk/client-sqs");
const config = {
  region: "us-east-1",
  endpoint,
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
  maxAttempts: 1,
};
const s3 = new S3Client({ ...config, forcePathStyle: true });
const ssm = new SSMClient(config);
const lambda = new LambdaClient(config);
const dynamo = new DynamoDBClient(config);
const sqs = new SQSClient(config);
async function inventory() {
  const [buckets, tables, queues, functions, parameters] = await Promise.all([
    s3.send(new ListBucketsCommand({})),
    dynamo.send(new ListTablesCommand({})),
    sqs.send(new ListQueuesCommand({})),
    lambda.send(new ListFunctionsCommand({})),
    ssm.send(new DescribeParametersCommand({})),
  ]);
  return {
    buckets: (buckets.Buckets || []).map((x) => x.Name).sort(),
    tables: (tables.TableNames || []).sort(),
    queues: (queues.QueueUrls || []).sort(),
    functions: (functions.Functions || []).map((x) => x.FunctionName).sort(),
    parameters: (parameters.Parameters || []).map((x) => x.Name).sort(),
  };
}
async function start(cwd, label) {
  const log = `/work/${label}.log`;
  const child = spawn(
    cli,
    ["dev", "--floci", "--mode=basic", "--stage", stage],
    {
      cwd,
      env: process.env,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let output = "";
  const writes = [];
  for (const stream of [child.stdout, child.stderr])
    stream.on("data", (chunk) => {
      output += chunk.toString();
      process.stdout.write(chunk);
      writes.push(appendFile(log, chunk));
    });
  let exit;
  child.on("error", (error) => {
    exit = error;
  });
  child.on("exit", (code) => {
    exit = code ?? "signal";
  });
  return {
    output: () => output,
    check: () => {
      if (exit !== undefined)
        throw new Error(`sst dev exited (${exit}): ${output.slice(-4000)}`);
    },
    async stop() {
      try {
        process.kill(-child.pid, "SIGINT");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
      await Promise.race([
        new Promise((resolve) => child.once("exit", resolve)),
        sleep(5000),
      ]);
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
      await Promise.all(writes);
    },
  };
}
const before = await inventory();
assert.deepEqual(
  before,
  { buckets: [], tables: [], queues: [], functions: [], parameters: [] },
  "CI requires a fresh isolated emulator",
);
for (const [kind, body] of Object.entries(negativeRuns)) {
  for (const name of await readdir(`${negative}/.sst`)) {
    if (name.startsWith("floci-preflight") && name.endsWith(".json"))
      await rm(`${negative}/.sst/${name}`);
  }
  await writeFile(`${negative}/sst.config.ts`, negativeConfig(body));
  const dev = await start(negative, `negative-${kind}`);
  try {
    await until(
      `negative ${kind} rejection`,
      () => /No AWS fallback was attempted/.test(dev.output()),
      90_000,
    );
    const reports = (await readdir(`${negative}/.sst`)).filter(
      (name) => name.startsWith("floci-preflight") && name.endsWith(".json"),
    );
    assert.ok(reports.length > 0, "preflight produced no report");
    const report = JSON.parse(
      await readFile(`${negative}/.sst/${reports[0]}`, "utf8"),
    );
    assert.ok(
      report.violations?.length > 0,
      `negative ${kind} did not report a profile violation`,
    );
  } finally {
    await dev.stop();
  }
  assert.deepEqual(
    await inventory(),
    before,
    `negative ${kind} created emulator resources`,
  );
}
for (const forbidden of ["deploy", "diff", "refresh"]) {
  const result = spawnSync(cli, [forbidden, "--stage", stage], {
    cwd: app,
    env: process.env,
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}${result.stderr}`, /Floci is only for sst dev/);
}
await rm(`${app}/.sst/outputs.json`, {force:true});
const dev = await start(app, "positive");
try {
  const outputs = await until(
    "app outputs",
    async () => {
      dev.check();
      const value = JSON.parse(
        await readFile(`${app}/.sst/outputs.json`, "utf8"),
      );
      return (
        typeof value.url === "string" &&
        value.url.startsWith("http") &&
        value.bucket &&
        value.table &&
        value
      );
    },
    180_000,
  );
  // The function host is virtual; route it over loopback while retaining Host.
  const functionURL = new URL(outputs.url);
  const localURL = new URL(outputs.url);
  localURL.hostname = "127.0.0.1";
  async function invoke() {
    const response = await request(localURL, {
      headers: { Host: functionURL.host },
    });
    assert.equal(response.status, 200, await response.clone().text());
    return response.json();
  }
  await until("live function response", async () => {
    dev.check();
    return (await invoke()).message === "hello from local SST";
  });
  const object = await s3.send(
    new GetObjectCommand({ Bucket: outputs.bucket, Key: "hello.txt" }),
  );
  assert.equal(await object.Body.transformToString(), "hello from local SST");
  for (const id of ["request", "subscriber"]) {
    await until(`${id} DynamoDB write`, async () => {
      dev.check();
      const item = await dynamo.send(
        new GetItemCommand({
          TableName: outputs.table,
          Key: { id: { S: id } },
        }),
      );
      return item.Item?.message?.S === "hello from local SST";
    });
  }
  const original = await readFile(`${app}/api.ts`, "utf8");
  await writeFile(
    `${app}/api.ts`,
    original.replaceAll("hello from local SST", "hello after reload"),
  );
  await until("live source reload", async () => {
    dev.check();
    return (await invoke()).message === "hello after reload";
  });
  await writeFile(
    "/work/result.json",
    JSON.stringify(
      {
        outputs,
        resources: await inventory(),
        negativeCases: Object.keys(negativeRuns),
        egressDenied: true,
        liveReload: true,
      },
      null,
      2,
    ),
  );
  console.log(
    "Floci integration passed: preflight, isolation, live function, S3, DynamoDB, SQS subscriber, reload.",
  );
} finally {
  await dev.stop();
}

// Returning to Floci must reuse its instance and stage state.
const identityBefore = await ssm.send(
  new GetParameterCommand({ Name: "/sst/floci/instance" }),
);
const result = JSON.parse(await readFile("/work/result.json", "utf8"));
const restarted = await start(app, "restart");
try {
  await until(
    "return to Floci",
    async () => {
      restarted.check();
      const originalURL = new URL(result.outputs.url);
      const localURL = new URL(originalURL);
      localURL.hostname = "127.0.0.1";
      const response = await request(localURL, {
        headers: { Host: originalURL.host },
      });
      return (
        response.ok && (await response.json()).message === "hello after reload"
      );
    },
    180_000,
  );
  const identityAfter = await ssm.send(
    new GetParameterCommand({ Name: "/sst/floci/instance" }),
  );
  assert.equal(identityAfter.Parameter.Value, identityBefore.Parameter.Value);
  assert.deepEqual(
    await inventory(),
    result.resources,
    "returning to Floci replaced resources",
  );
  result.restart = true;
  await writeFile("/work/result.json", JSON.stringify(result, null, 2));
  console.log(
    "Floci restart passed: instance identity, stage state, and live function reused.",
  );
} finally {
  await restarted.stop();
}
