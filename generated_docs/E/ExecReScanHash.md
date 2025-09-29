# ExecReScanHash

## Location
[src/backend/executor/nodeHash.c:2360-2381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2360-L2381)

## Overview
Rescans a Hash node by conditionally rescanning its outer plan, used when hash join operations need to be restarted or reprocessed.

## Definition
void ExecReScanHash(HashState *node)

## Detailed Description
This function implements the rescan functionality for Hash plan nodes in PostgreSQL's executor. When a hash join operation needs to be rescanned (typically due to parameter changes in nested loop scenarios), this function determines whether the outer plan needs to be rescanned.

The function follows PostgreSQL's parameter change optimization: if the outer plan has changed parameters (chgParam is not NULL), the plan will automatically be rescanned on the next ExecProcNode call, so no explicit rescan is needed. If no parameters have changed (chgParam is NULL), the function explicitly rescans the outer plan to restart data flow.

This approach ensures efficient handling of nested scenarios where the same hash table might be reused multiple times with different parameter values.

## Parameters / Member Variables
- : HashState execution node containing the hash table and outer plan state information

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState - macro to get the outer plan state from the Hash node
  - [ExecReScan](ExecReScan.md) - general-purpose rescan function for plan nodes
  - [PlanState](../P/PlanState.md) - base structure for plan node execution state
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) - generic executor rescan dispatcher function

## Notes and Other Information
- Part of PostgreSQL's executor rescan infrastructure for plan nodes
- Implements parameter change optimization to avoid unnecessary rescans
- Only rescans the outer plan that feeds data into the hash table
- Does not directly manipulate the hash table itself - that's handled by the hash join node
- Essential for proper nested loop join functionality where inner hash joins are rescanned
- The rescan behavior is conditional based on parameter change detection (chgParam field)

## Simplified Source

```c
void ExecReScanHash(HashState *node) {
    PlanState *outerPlan = outerPlanState(node);

    // Only rescan outer plan if no parameter changes detected
    // If parameters changed, rescan will happen automatically on next ExecProcNode call
    if (outerPlan->chgParam == NULL) {
        ExecReScan(outerPlan);
    }
}
```