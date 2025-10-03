# recompute_limits

## Location
[src/backend/executor/nodeLimit.c:353-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L353-L430)

## Overview
recompute_limits evaluates LIMIT and OFFSET expressions at node startup or rescan, converting them to concrete numeric values and resetting the limit node's execution state.

## Definition

```c
static void
recompute_limits(LimitState *node)
```
## Detailed Description
This function is responsible for computing the actual offset and count values from potentially parameterized LIMIT and OFFSET expressions. It is called during node initialization and rescans when parameters may have changed. The function evaluates the expressions in the current expression context, handles NULL values appropriately, validates that the values are non-negative, and resets the node's position tracking state.

Key behaviors include:
- OFFSET expressions: NULL is interpreted as offset 0 (no offset)
- LIMIT expressions: NULL is interpreted as no limit (equivalent to LIMIT ALL)
- Validation ensures both OFFSET and LIMIT values are non-negative
- The function resets execution state to LIMIT_RESCAN and position to 0
- Notifies the child node about the tuple bound requirement for optimization

## Parameters / Member Variables
- `*node`: LimitState containing the limit expressions, execution context, and state variables to update
## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md) (evaluates LIMIT/OFFSET expressions)
  - [DatumGetInt64](../D/DatumGetInt64.md) (extracts int64 values from expression results)
  - [ExecSetTupleBound](../E/ExecSetTupleBound.md) (notifies child about required tuple count)
  - [compute_tuples_needed](../c/compute_tuples_needed.md) (calculates optimal tuple bound for child)
  - outerPlanState (accesses child plan state)
- Called from (representative examples):
  - [ExecLimit](../E/ExecLimit.md) (during LIMIT_INITIAL state)
  - [ExecReScanLimit](../E/ExecReScanLimit.md) (when rescanning the limit node)

## Notes and Other Information
- Handles parameterized queries where LIMIT/OFFSET values may change between executions
- NULL handling follows SQL standard semantics (NULL OFFSET = 0, NULL LIMIT = unlimited)
- Error reporting includes specific error codes for invalid row counts
- Always calls ExecSetTupleBound even if compute_tuples_needed returns -1 to ensure child nodes are properly updated during rescans
- The function switches expression context during evaluation to ensure proper memory management

## Simplified Source

```c
static void recompute_limits(LimitState *node) {
    ExprContext *econtext = node->ps.ps_ExprContext;
    Datum val;
    bool isNull;

    // Evaluate OFFSET expression
    if (node->limitOffset) {
        val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &isNull);
        if (isNull) {
            node->offset = 0;  // NULL OFFSET means no offset
        } else {
            node->offset = DatumGetInt64(val);
            if (node->offset < 0)
                ereport(ERROR, "OFFSET must not be negative");
        }
    } else {
        node->offset = 0;  // No OFFSET clause
    }

    // Evaluate LIMIT expression
    if (node->limitCount) {
        val = ExecEvalExprSwitchContext(node->limitCount, econtext, &isNull);
        if (isNull) {
            node->count = 0;
            node->noCount = true;  // NULL LIMIT means no limit (LIMIT ALL)
        } else {
            node->count = DatumGetInt64(val);
            if (node->count < 0)
                ereport(ERROR, "LIMIT must not be negative");
            node->noCount = false;
        }
    } else {
        node->count = 0;
        node->noCount = true;  // No LIMIT clause
    }

    // Reset execution state for new scan
    node->position = 0;
    node->subSlot = NULL;
    node->lstate = LIMIT_RESCAN;

    // Notify child node about tuple bound for optimization
    ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
}
```