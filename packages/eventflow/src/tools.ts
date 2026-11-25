import { NonRetriableError, RetryAfterError } from "./errors.js";

// What step.fetch and step.ai.infer do inside their step.

/** The largest response, in bytes, that's saved: a step's result goes in the run's history. */
const MAX_BODY = 700 * 1024;

export interface SavedResponse {
  status: number;
  statusText: string;
  headers: [string, string][];
  /** Text, or base64. */
  body?: string;
  base64?: boolean;
}

export async function saveResponse(response: Response): Promise<SavedResponse> {
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length > MAX_BODY)
    throw new NonRetriableError(`The response is ${bytes.length} bytes. A step saves at most ${MAX_BODY}. Store it in a bucket in a step.run, and return its key.`);
  const headers: [string, string][] = [];
  response.headers.forEach((value, name) => headers.push([name, value]));
  const text = bytes.toString("utf8");
  const isText = Buffer.from(text, "utf8").equals(bytes);
  return {
    status: response.status,
    statusText: response.statusText,
    headers,
    ...(bytes.length ? (isText ? { body: text } : { body: bytes.toString("base64"), base64: true }) : {}),
  };
}

export function toResponse(saved: SavedResponse) {
  const body = saved.body === undefined ? null : saved.base64 ? Buffer.from(saved.body, "base64") : saved.body;
  // The body is saved as it was decoded, whatever its encoding was.
  const headers = saved.headers.filter(([k]) => !/^(content-encoding|content-length|transfer-encoding)$/i.test(k));
  return new Response(saved.status === 204 || saved.status === 304 ? null : body, { status: saved.status, statusText: saved.statusText, headers });
}

/** A model adapter, like one of `@inngest/ai`'s. */
export interface AiModel {
  format?: string;
  url?: string;
  headers?: Record<string, string>;
  authKey?: string;
  options?: any;
  onCall?: (model: any, body: any) => void;
}

function authHeaders(model: AiModel): Record<string, string> {
  if (!model.authKey) return {};
  switch (model.format) {
    case "anthropic":
      return { "x-api-key": model.authKey };
    case "gemini":
      return { "x-goog-api-key": model.authKey };
    case "azure-openai":
      return { "api-key": model.authKey };
    default:
      return { authorization: `Bearer ${model.authKey}` };
  }
}

/** Call a model. A rate limit, or an error of the provider's, is tried again; a bad request isn't. */
export async function infer(input: { model: AiModel; body: any }, fetcher: typeof fetch = fetch): Promise<unknown> {
  const model: AiModel = { ...input.model, headers: { ...input.model?.headers } };
  const body = structuredClone(input.body ?? {});
  model.onCall?.(model, body);
  if (!model.url) throw new NonRetriableError("step.ai.infer needs a model with a `url`, like one from @inngest/ai.");
  const response = await fetcher(model.url, {
    method: "POST",
    headers: { "content-type": "application/json", ...authHeaders(model), ...model.headers },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (response.ok) {
    if (Buffer.byteLength(text) > MAX_BODY) throw new NonRetriableError(`The model's answer is over ${MAX_BODY} bytes, more than a step saves.`);
    return JSON.parse(text);
  }
  // Not the whole of it, which can have the prompt in it.
  const message = `The model answered ${response.status}: ${text.slice(0, 300)}`;
  if (response.status === 408 || response.status === 409 || response.status === 429 || response.status >= 500) {
    const after = response.headers.get("retry-after");
    const seconds = after === null ? NaN : Number(after);
    if (Number.isFinite(seconds) && seconds >= 0) throw new RetryAfterError(message, seconds * 1000);
    throw new Error(message);
  }
  throw new NonRetriableError(message);
}
