import { awsFetch, type AwsOptions } from "./client.js";

// The few durable execution calls the engine makes. They're here, not taken from
// `sst/aws/workflow`, so this package doesn't depend on the version of SST.

const enc = encodeURIComponent;

/** A call to Lambda that it answered with an error. */
export class LambdaError extends Error {
  constructor(
    action: string,
    public readonly response: Response,
  ) {
    super(`Lambda ${action} failed: ${response.status}`);
    this.name = "LambdaError";
  }
}

export interface Started {
  arn?: string;
  version?: string;
}

/** Start a durable execution. Lambda starts one per name. */
export async function startExecution(
  fn: { name: string; qualifier: string },
  input: { name: string; payload: unknown },
  aws?: AwsOptions,
): Promise<Started> {
  const response = await awsFetch(
    "lambda",
    `/2015-03-31/functions/${enc(fn.name)}/invocations?Qualifier=${enc(fn.qualifier)}`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Amz-Durable-Execution-Name": input.name,
        "X-Amz-Invocation-Type": "Event",
      },
      body: JSON.stringify(input.payload),
    },
    { aws },
  );
  if (!response.ok) throw new LambdaError("Invoke", response);
  return {
    arn: response.headers.get("X-Amz-Durable-Execution-Arn") ?? undefined,
    version: response.headers.get("X-Amz-Executed-Version") ?? undefined,
  };
}

/** Stop a durable execution. */
export async function stopExecution(arn: string, aws?: AwsOptions) {
  const response = await awsFetch("lambda", `/2025-12-01/durable-executions/${enc(arn)}/stop`, { method: "POST" }, { aws });
  if (!response.ok) throw new LambdaError("StopDurableExecution", response);
}

/** Complete a durable execution's callback. */
export async function succeedCallback(id: string, payload: unknown, aws?: AwsOptions) {
  const response = await awsFetch(
    "lambda",
    `/2025-12-01/durable-execution-callbacks/${enc(id)}/succeed`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload === undefined ? undefined : JSON.stringify(payload),
    },
    { aws },
  );
  if (!response.ok) throw new LambdaError("SendDurableExecutionCallbackSuccess", response);
}
