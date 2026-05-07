import type { StressDerivedReport } from './types.js';

export function formatProductionReadinessFailure(report: StressDerivedReport): string {
  const failed = report.productionReadiness.failedRequirements
    .map((requirement) =>
      `${requirement.id}: required ${requirement.required}, actual ${requirement.actual}`
    )
    .join('; ');
  return `Production readiness gate failed for ${report.runId}: ${failed}`;
}

export function assertProductionReadiness(report: StressDerivedReport): void {
  if (report.productionReadiness.status !== 'pass') {
    throw new Error(formatProductionReadinessFailure(report));
  }
}
