export type ReconciliationMode = "dry-run" | "apply";

export type ReconciliationStatus =
  | "dry_run"
  | "applied"
  | "failed_closed"
  | "unauthorized"
  | "not_configured"
  | "kill_switch_active";

export interface ApprovalReconciliationRequest {
  readonly mode: ReconciliationMode;
  readonly runId?: string;
  readonly reason?: string;
  readonly maxItems?: number;
  readonly minAgeSeconds?: number;
  readonly protectedConfirmation?: string;
}

export interface ApprovalReconciliationCandidate {
  readonly outboxId: string;
  readonly idempotencyKey: string;
  readonly destinationStage: string;
  readonly routingKey: string;
  readonly entityKind: string;
  readonly entityId: string;
  readonly payloadRef: string;
  readonly payloadDigest: string;
  readonly selectedReason: string;
  readonly status: "selected" | "replayed" | "failed_closed";
  readonly replayMessageId?: string;
  readonly failedClosedReason?: string;
}

export interface ApprovalReconciliationReport {
  readonly service: "approval";
  readonly mode: ReconciliationMode;
  readonly status: ReconciliationStatus;
  readonly requestedAt: string;
  readonly runId?: string;
  readonly reason?: string;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly selectedCount: number;
  readonly replayedCount: number;
  readonly failedClosedCount: number;
  readonly skippedCount: number;
  readonly writesPerformed: boolean;
  readonly dryRun: boolean;
  readonly productionVisibilityEnabled: false;
  readonly legacyRuntimeRequired: false;
  readonly protectedApplyRequired: true;
  readonly candidates: readonly ApprovalReconciliationCandidate[];
  readonly errors: readonly string[];
  readonly metrics: {
    readonly candidateCount: number;
    readonly replayedCount: number;
    readonly failedClosedCount: number;
    readonly skippedCount: number;
  };
}

export interface ApprovalReconciler {
  readonly name: string;
  reconcile(request: ApprovalReconciliationRequest): Promise<ApprovalReconciliationReport>;
}

export const APPROVAL_RECONCILIATION_PATH = "/reconcile/outbox";
export const APPROVAL_RECONCILIATION_CONFIRMATION = "approval:replay-outbox:v1";
