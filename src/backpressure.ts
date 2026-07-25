export interface ApprovalQwenCapacityPermit {
  release(): void;
}

export interface ApprovalQwenCapacityLimiter {
  readonly activeCount: number;
  readonly queuedCount: number;
  acquire(): Promise<ApprovalQwenCapacityPermit | undefined>;
}

export interface ApprovalQwenCapacityLimiterOptions {
  readonly maxParallelCalls: number;
  readonly maxQueuedCalls: number;
}

type PermitResolver = (permit: ApprovalQwenCapacityPermit) => void;

export class InMemoryApprovalQwenCapacityLimiter implements ApprovalQwenCapacityLimiter {
  private active = 0;
  private readonly waiting: PermitResolver[] = [];

  constructor(private readonly options: ApprovalQwenCapacityLimiterOptions) {}

  get activeCount(): number {
    return this.active;
  }

  get queuedCount(): number {
    return this.waiting.length;
  }

  acquire(): Promise<ApprovalQwenCapacityPermit | undefined> {
    if (this.active < this.options.maxParallelCalls) {
      this.active += 1;

      return Promise.resolve(this.createPermit());
    }

    if (this.waiting.length >= this.options.maxQueuedCalls) {
      return Promise.resolve(undefined);
    }

    return new Promise((resolve) => {
      this.waiting.push(resolve);
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

    next(this.createPermit());
  }
}
