import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

export interface ApprovalDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface ApprovalStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
}

export interface ApprovalDatabaseTransaction {
  readonly transactionId: string;
}

export interface ApprovalDatabaseTransactionRunner {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  withTransaction<T>(operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface ApprovalBrokerOutbox {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  record(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
}

export interface ApprovalQwenClient {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
}

export interface ApprovalPrompt {
  readonly id: string;
  readonly version: string;
  readonly purpose: "editorial-approval";
}

export interface ApprovalPromptRegistry {
  readonly name: string;
  probe(): ApprovalDependencyProbe | Promise<ApprovalDependencyProbe>;
  getPrompt(id: string): Promise<ApprovalPrompt>;
}

export interface ApprovalWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
  recordOutbox(command: BrokerPublishCommand, receipt: BrokerPublishReceipt): Promise<void>;
  withTransaction<T>(operation: (transaction: ApprovalDatabaseTransaction) => Promise<T>): Promise<T>;
}

export interface ApprovalWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: ApprovalWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface ApprovalDependencies {
  readonly clock: RuntimeClock;
  readonly stateStore: ApprovalStateStore;
  readonly transactionRunner: ApprovalDatabaseTransactionRunner;
  readonly brokerOutbox: ApprovalBrokerOutbox;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly qwenClient: ApprovalQwenClient;
  readonly promptRegistry: ApprovalPromptRegistry;
  readonly workHandler: ApprovalWorkHandler;
}
