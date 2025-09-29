# ExecReScanProjectSet

## Location
[src/backend/executor/nodeProjectSet.c:337-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeProjectSet.c#L337-L350)

## Overview
ExecReScanProjectSet resets a ProjectSet node to restart execution from the beginning, handling SRF state cleanup and child node rescanning.

## Definition
```c
void ExecReScanProjectSet(ProjectSetState *node)
```

## Detailed Description
ExecReScanProjectSet implements the rescan functionality for ProjectSet nodes, which is necessary when a plan needs to be re-executed (such as in nested loop joins or other scenarios requiring multiple passes through the data).

The function performs two key operations:
1. **SRF State Reset**: Clears any incompletely-evaluated SRFs by setting `pending_srf_tuples` to false, ensuring that any partially-consumed set-returning functions start fresh
2. **Child Plan Rescanning**: Conditionally rescans the outer child plan, but only if parameter changes haven't already triggered a rescan

The conditional rescanning logic optimizes performance by avoiding redundant rescans when the executor framework has already scheduled a rescan due to parameter changes.

## Parameters / Member Variables
- `node`: The ProjectSetState to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecReScan](ExecReScan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (as part of plan tree rescanning)

## Notes and Other Information
- The chgParam check prevents double-rescanning when parameters have changed
- Resetting pending_srf_tuples is crucial for SRF correctness across rescans
- Part of the standard executor rescan protocol used throughout PostgreSQL's executor
- [ProjectSet](../P/ProjectSet.md) nodes don't maintain complex state that requires extensive cleanup during rescan

## Simplified Source

```c
void ExecReScanProjectSet(ProjectSetState *node) {
    PlanState *outerPlan = outerPlanState(node);

    // Clear any incompletely-evaluated SRFs
    node->pending_srf_tuples = false;

    // Rescan outer plan if no parameters changed
    if (outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);
}
```