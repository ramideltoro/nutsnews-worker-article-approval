export interface ApprovalQwenCapacityPermit {
  release(): void;
}

export interface ApprovalQwenCapacityLimiter {
  readonly activeCount: number;
  readonly queuedCount: number;
  acquire(signal?: AbortSignal): Promise<ApprovalQwenCapacityPermit | undefined>;
}

export interface ApprovalQwenCapacityLimiterOptions {
  readonly maxParallelCalls: number;
  readonly maxQueuedCalls: number;
}

interface PermitWaiter {
  readonly resolve: (permit: ApprovalQwenCapacityPermit) => void;
  readonly reject: (reason: unknown) => void;
  readonly signal?: AbortSignal;
  readonly onAbort?: () => void;
}

export class InMemoryApprovalQwenCapacityLimiter implements ApprovalQwenCapacityLimiter {
  private active = 0;
  private readonly waiting: PermitWaiter[] = [];

  constructor(private readonly options: ApprovalQwenCapacityLimiterOptions) {}

  get activeCount(): number {
    return this.active;
  }

  get queuedCount(): number {
    return this.waiting.length;
  }

  acquire(signal?: AbortSignal): Promise<ApprovalQwenCapacityPermit | undefined> {
    throwIfAborted(signal);

    if (this.active < this.options.maxParallelCalls) {
      this.active += 1;

      return Promise.resolve(this.createPermit());
    }

    if (this.waiting.length >= this.options.maxQueuedCalls) {
      return Promise.resolve(undefined);
    }

    return new Promise((resolve, reject) => {
      const waiter: PermitWaiter = {
        resolve,
        reject,
        ...(signal === undefined ? {} : {
          signal,
          onAbort: () => {
            const index = this.waiting.indexOf(waiter);

            if (index !== -1) {
              this.waiting.splice(index, 1);
            }
            reject(abortReason(signal));
          }
        })
      };

      this.waiting.push(waiter);
      if (signal !== undefined && waiter.onAbort !== undefined) {
        signal.addEventListener("abort", waiter.onAbort, {
          once: true
        });
        if (signal.aborted) {
          waiter.onAbort();
        }
      }
    });
  }

  private createPermit(): ApprovalQwenCapacityPermit {
    let released = false;

    return {
      release: () => {
        if (released) {
          return;
        }

        released = true;
        this.release();
      }
    };
  }

  private release(): void {
    const next = this.waiting.shift();

    if (next === undefined) {
      this.active = Math.max(0, this.active - 1);
      return;
    }

    if (next.onAbort !== undefined) {
      next.signal?.removeEventListener("abort", next.onAbort);
    }
    next.resolve(this.createPermit());
  }
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw abortReason(signal);
  }
}

function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error("Approval Qwen capacity wait was aborted.");
}
