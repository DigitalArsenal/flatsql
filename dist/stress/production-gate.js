export function formatProductionReadinessFailure(report) {
    const failed = report.productionReadiness.failedRequirements
        .map((requirement) => `${requirement.id}: required ${requirement.required}, actual ${requirement.actual}`)
        .join('; ');
    return `Production readiness gate failed for ${report.runId}: ${failed}`;
}
export function assertProductionReadiness(report) {
    if (report.productionReadiness.status !== 'pass') {
        throw new Error(formatProductionReadinessFailure(report));
    }
}
//# sourceMappingURL=production-gate.js.map