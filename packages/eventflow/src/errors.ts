// The errors a function throws and gets.

export class NonRetriableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options as any);
    this.name = "NonRetriableError";
  }
}

export class RetryAfterError extends Error {
  public readonly retryAfter: Date | number | string;
  constructor(message: string, retryAfter: number | string | Date, options?: { cause?: unknown }) {
    super(message, options as any);
    this.name = "RetryAfterError";
    this.retryAfter = retryAfter;
  }
}

/** The error a step throws once it has used up its retries. */
export class StepFailedError extends Error {
  constructor(
    public readonly stepId: string,
    public readonly cause: { name: string; message: string },
  ) {
    super(cause.message);
    this.name = cause.name === "Error" ? "StepError" : cause.name;
  }
}

export class InvokeFailedError extends Error {
  constructor(
    message: string,
    public readonly functionId: string,
  ) {
    super(message);
    this.name = "InvokeError";
  }
}

/** The error a wait for a signal throws while another run waits for it. */
export class SignalConflictError extends NonRetriableError {
  constructor(signal: string) {
    super(`Another run waits for the signal "${signal}".`);
    this.name = "SignalConflictError";
  }
}
