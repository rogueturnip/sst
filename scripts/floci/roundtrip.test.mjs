import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { classifyURL, options, targetEnvironment } from "./roundtrip.mjs";

const explicit = {
  SST_TEST_AWS_PROFILE: "integration-sandbox",
  SST_TEST_STAGE: "roundtrip-test",
  SST_TEST_CLI: "/tmp/sst-test",
};

test("missing opt-ins fail before creating files or invoking any executable", () => {
  for (const omitted of Object.keys(explicit)) {
    const env = { ...explicit, PATH: "/no-executables-can-run" };
    delete env[omitted];
    const child = spawnSync(
      process.execPath,
      [fileURLToPath(new URL("./roundtrip.mjs", import.meta.url))],
      { env, encoding: "utf8" },
    );
    assert.equal(child.status, 1);
    assert.match(child.stderr, new RegExp(`${omitted} must be explicitly set`));
    assert.equal(
      child.stdout,
      "",
      "no workspace or target should have been started",
    );
  }
});

test("AWS session cannot inherit local endpoints, dummy keys or dead proxies", () => {
  const contaminated = {
    HOME: "/home/developer",
    PATH: "/usr/bin",
    CUSTOM_APP_SETTING: "retained",
    AWS_ACCESS_KEY_ID: "test",
    AWS_SECRET_ACCESS_KEY: "test",
    AWS_SESSION_TOKEN: "dummy",
    AWS_PROFILE: "unrelated-production",
    AWS_DEFAULT_PROFILE: "unrelated-production",
    AWS_CONFIG_FILE: "/dev/null",
    AWS_SHARED_CREDENTIALS_FILE: "/dev/null",
    AWS_ENDPOINT_URL: "http://localhost:4566",
    AWS_ENDPOINT_URL_S3: "http://localhost:4567",
    SST_AWS_ACCESS_KEY_ID: "test",
    SST_AWS_REGION: "wrong-region",
    SST_FLOCI: "1",
    SST_FLOCI_ENDPOINT: "http://localhost:4566",
    SST_FLOCI_BRIDGE: "http://host.docker.internal:1234",
    SST_FLOCI_TOKEN: "secret",
    SST_FLOCI_PREFLIGHT: "1",
    SST_FLOCI_REPORT: "/stale-report",
    HTTPS_PROXY: "http://localhost:1",
    http_proxy: "http://localhost:1",
    NO_PROXY: "*",
    NODE_USE_ENV_PROXY: "1",
  };
  const cloud = targetEnvironment(contaminated, "aws", options(explicit));
  assert.deepEqual(cloud, {
    HOME: "/home/developer",
    PATH: "/usr/bin",
    CUSTOM_APP_SETTING: "retained",
    AWS_PROFILE: "integration-sandbox",
    AWS_REGION: "us-east-1",
    AWS_DEFAULT_REGION: "us-east-1",
    AWS_IGNORE_CONFIGURED_ENDPOINT_URLS: "true",
  });
  const local = targetEnvironment(
    cloud,
    "floci",
    options({ ...explicit, SST_TEST_FLOCI_ENDPOINT: "http://127.0.0.1:4567" }),
  );
  assert.equal(local.SST_FLOCI_ENDPOINT, "http://127.0.0.1:4567");
  assert.equal(local.AWS_PROFILE, undefined);
  assert.equal(
    local.AWS_ACCESS_KEY_ID,
    undefined,
    "CLI supplies isolated credentials, not the runner",
  );
});

test("target classification catches stale opposite-target outputs and spoofed hosts", () => {
  assert.equal(classifyURL("https://abc.lambda-url.us-east-1.on.aws/"), "aws");
  assert.equal(
    classifyURL("http://abc.lambda-url.us-east-1.localhost.floci.io:4567/"),
    "floci",
  );
  for (const url of [
    "https://abc.lambda-url.us-east-1.on.aws.attacker.test/",
    "https://localhost.floci.io.attacker.test/",
    "http://abc.lambda-url.us-east-1.on.aws/",
    "https://example.com/",
  ])
    assert.throws(() => classifyURL(url), /unrecognized/);
});

test("invalid local endpoint and stage fail before target selection", () => {
  for (const endpoint of [
    "https://127.0.0.1:4566",
    "http://aws.example:4566",
    "http://user:pass@localhost:4566",
    "http://localhost:4566/path",
  ])
    assert.throws(
      () => options({ ...explicit, SST_TEST_FLOCI_ENDPOINT: endpoint }),
      /loopback/,
    );
  assert.throws(
    () => options({ ...explicit, SST_TEST_STAGE: "../prod" }),
    /stage|STAGE/,
  );
});

test("invocation checks actual HTTP success and body rather than stale files", async () => {
  const { createServer } = await import("node:http");
  const { invokeURL } = await import("./roundtrip.mjs");
  const server = createServer((request, response) => {
    response.writeHead(request.url === "/error" ? 500 : 200);
    response.end(JSON.stringify({ message: "from live function" }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const url = `http://127.0.0.1:${server.address().port}`;
    assert.deepEqual(await invokeURL(url, AbortSignal.timeout(1000)), {
      message: "from live function",
    });
    await assert.rejects(invokeURL(url + "/error", AbortSignal.timeout(1000)));
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
