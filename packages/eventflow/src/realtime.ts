import { client, type AwsOptions } from "./client.js";

// Messages to browsers, as Inngest's realtime, published to SST's `Realtime` component: AWS
// IoT. A browser subscribes with its MQTT client, and the component's authorizer says to
// what. Inngest's subscription tokens and `useRealtime` hook aren't here.

export interface TopicRef<TData = any> {
  channel: string;
  topic: string;
  config: { schema?: any };
  /** @internal */
  readonly __data?: TData;
}

/** What a `Realtime` component gives, by its link. */
export interface RealtimeResource {
  endpoint: string;
  authorizer?: string;
}

/** The largest message IoT takes is 128 KB. */
const MAX_BYTES = 120 * 1024;

/**
 * Declare a channel, with its topics, as Inngest's `channel()`.
 *
 * @example
 * ```ts
 * const order = channel({ name: (id: string) => `order:${id}`, topics: { status: { schema: z.object({ state: z.string() }) } } });
 *
 * await step.realtime.publish("tell", order(orderId).status, { state: "paid" });
 * ```
 */
export function channel<TArgs extends any[], TTopics extends Record<string, { schema?: any }>>(options: {
  name: string;
  topics: TTopics;
}): { name: string; topics: TTopics } & { [K in keyof TTopics]: TopicRef };
export function channel<TArgs extends any[], TTopics extends Record<string, { schema?: any }>>(options: {
  name: (...args: TArgs) => string;
  topics: TTopics;
}): ((...args: TArgs) => { name: string; topics: TTopics } & { [K in keyof TTopics]: TopicRef }) & { topics: TTopics };
export function channel(options: { name: string | ((...args: any[]) => string); topics: Record<string, { schema?: any }> }): any {
  const make = (name: string) => ({
    ...Object.fromEntries(Object.entries(options.topics).map(([topic, config]) => [topic, { channel: name, topic, config }])),
    name,
    topics: options.topics,
  });
  const { name } = options;
  if (typeof name === "string") return make(name);
  return Object.assign((...args: any[]) => make(name(...args)), { topics: options.topics });
}

/** A part of an MQTT topic: without what's special in one. */
function part(text: string) {
  return encodeURIComponent(text).replace(/\*/g, "%2A");
}

/** The MQTT topic of a channel's topic, for a browser to subscribe to, and an authorizer to allow. */
export function mqttTopic(prefix: string, ref: { channel: string; topic: string }) {
  return `${prefix}/${part(ref.channel)}/${part(ref.topic)}`;
}

export async function publish(
  resource: RealtimeResource,
  prefix: string,
  ref: TopicRef,
  data: unknown,
  about: { runId?: string; fnId?: string },
  aws?: AwsOptions,
) {
  if (!ref || typeof ref.channel !== "string" || typeof ref.topic !== "string")
    throw new TypeError("realtime.publish() needs a topic of a channel, like channel(id).status.");
  const schema = ref.config?.schema?.["~standard"];
  if (schema) {
    const result = await schema.validate(data);
    if (result.issues)
      throw new TypeError(`The message to "${ref.topic}" doesn't match its schema: ${result.issues.map((i: any) => i.message).join("; ")}`);
    data = result.value;
  }
  const body = JSON.stringify({ channel: ref.channel, topic: ref.topic, data, kind: "data", createdAt: new Date().toISOString(), ...about });
  if (Buffer.byteLength(body) > MAX_BYTES) throw new TypeError(`The message to "${ref.topic}" is over ${MAX_BYTES} bytes.`);
  const topic = mqttTopic(prefix, ref);
  if (topic.length > 256 || topic.split("/").length > 8) throw new TypeError(`The topic "${topic.slice(0, 80)}" is longer than IoT takes.`);
  const c = await client(aws);
  // Signed as it's sent: the topic is one part of the path.
  const response = await c.fetch(`https://${resource.endpoint}/topics/${encodeURIComponent(topic)}?qos=1`, {
    method: "POST",
    body,
    aws: { ...aws, service: "iotdata" },
  });
  if (!response.ok) throw new Error(`Failed to publish to "${topic}": ${response.status} ${(await response.text()).slice(0, 300)}`);
  return data;
}
