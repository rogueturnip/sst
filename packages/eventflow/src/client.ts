import { AwsClient } from "aws4fetch";

interface EC2Credentials {
  AccessKeyId: string;
  SecretAccessKey: string;
  Token: string;
  Expiration: string;
  Type: string;
}

let cachedCredentials: EC2Credentials | null = null;

async function getCredentials(url: string): Promise<EC2Credentials> {
  if (cachedCredentials) {
    const currentTime = new Date();
    const fiveMinutesFromNow = new Date(currentTime.getTime() + 5 * 60000);
    const expirationTime = new Date(cachedCredentials.Expiration);
    if (expirationTime > fiveMinutesFromNow) {
      return cachedCredentials;
    }
  }

  const credentials = (await fetch(url).then((res) =>
    res.json(),
  )) as EC2Credentials;
  cachedCredentials = credentials;
  return credentials;
}

type AwsFetchOptions = Exclude<Parameters<AwsClient["fetch"]>[1], null | undefined>;

export type AwsOptions = Exclude<
  Parameters<AwsClient["fetch"]>[1],
  null | undefined
>["aws"];

export async function client(aws?: AwsOptions): Promise<AwsClient> {
  if (aws?.accessKeyId && aws.secretAccessKey) {
    return new AwsClient({
      accessKeyId: aws.accessKeyId,
      secretAccessKey: aws.secretAccessKey,
      sessionToken: aws.sessionToken,
      service: aws.service,
      region: aws.region ?? process.env.AWS_REGION,
      cache: aws.cache,
    });
  }

  if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
    return new AwsClient({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      sessionToken: process.env.AWS_SESSION_TOKEN,
      region: process.env.AWS_REGION,
    });
  }

  if (process.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI) {
    const credentials = await getCredentials(
      "http://169.254.170.2" +
        process.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI,
    );
    return new AwsClient({
      accessKeyId: credentials.AccessKeyId,
      secretAccessKey: credentials.SecretAccessKey,
      sessionToken: credentials.Token,
      region: process.env.AWS_REGION,
    });
  }

  throw new Error("No AWS credentials found");
}

export async function awsFetch(
  service: string,
  path: string,
  init: Omit<AwsFetchOptions, "aws">,
  options?: {
    aws?: AwsOptions;
    /** How many times a 5xx or a 429 is tried again. Defaults to aws4fetch's 10. */
    retries?: number;
  },
) {
  const c = await client(options?.aws);
  const retries = options?.retries ?? 10;
  // Tried again here, not by aws4fetch, so a throttle or an outage is seen in the logs, and
  // isn't only a call that's slow.
  c.retries = 0;
  const region = options?.aws?.region ?? c.region;
  const url = `https://${service}.${region}.amazonaws.com${path}`;
  const began = Date.now();
  let last: string | number = "";
  for (let attempt = 0; ; attempt++) {
    let response: Response | undefined;
    try {
      response = await c.fetch(url, { ...init, aws: options?.aws });
    } catch (error) {
      // A network error is thrown, as aws4fetch does: the callers that may try again, like
      // DynamoDB's and a callback's, have their own tries for it.
      if (attempt) tell(service, path, String((error as Error)?.name ?? error), attempt + 1, began, true);
      throw error;
    }
    if (response && !(response.status === 429 || response.status >= 500)) {
      if (attempt) tell(service, path, last, attempt + 1, began, false);
      return response;
    }
    if (response) {
      last = response.status;
      if (attempt >= retries) {
        tell(service, path, last, attempt + 1, began, true);
        return response;
      }
      await response.body?.cancel().catch(() => {});
    }
    // As aws4fetch: from 50 ms, doubling, with jitter.
    await new Promise((r) => setTimeout(r, Math.random() * 50 * 2 ** attempt));
  }
}

/** One line for a call that AWS throttled, or that failed and was tried again. */
function tell(service: string, path: string, status: string | number, tries: number, began: number, gaveUp: boolean) {
  console.warn(
    JSON.stringify({
      level: "warn",
      message: gaveUp ? "An AWS call failed after its tries" : "An AWS call was tried again",
      service,
      path: path.split("?")[0].slice(0, 120),
      status,
      tries,
      ms: Date.now() - began,
    }),
  );
}
