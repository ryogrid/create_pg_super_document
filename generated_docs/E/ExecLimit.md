# ExecLimit

## Location
[src/backend/executor/nodeLimit.c:40-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L40-L352)

## Overview
ExecLimit implements the execution logic for LIMIT/OFFSET filtering, managing a state machine that controls tuple flow from a subplan to enforce row count limits and offset requirements.

## Definition

```c
static TupleTableSlot *			/* return: a tuple or NULL */
ExecLimit(PlanState *pstate)
```
## Detailed Description
ExecLimit is the main execution function for PostgreSQL's LIMIT node, implementing a sophisticated state machine to handle various LIMIT/OFFSET scenarios including support for WITH TIES semantics. The function processes tuples from its subplan and applies filtering based on computed offset and count values.

The state machine handles multiple execution states:
- **LIMIT_INITIAL**: First call, computes limit/offset parameters
- **LIMIT_RESCAN**: Resets to start of result window
- **LIMIT_INWINDOW**: Normal processing within the limit window
- **LIMIT_EMPTY**: No tuples to return (empty window or subplan exhausted)
- **LIMIT_WINDOWEND**: Reached end of limit window
- **LIMIT_WINDOWEND_TIES**: Processing ties at window boundary (WITH TIES)
- **LIMIT_SUBPLANEOF**: Subplan reached EOF
- **LIMIT_WINDOWSTART**: Backing off from window start

The function supports both forward and backward scanning directions and handles the complex logic for WITH TIES, which requires comparing tuples to determine if they have equivalent ORDER BY values.

## Parameters / Member Variables
- : Plan state containing the LimitState node and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [recompute_limits](../r/recompute_limits.md) (computes offset/count on first call)
  - ExecProcNode (fetches tuples from subplan)
  - ExecCopySlot (saves tuple for WITH TIES comparison)  
  - ExecQualAndReset (compares tuples for WITH TIES logic)
  - ScanDirectionIsForward (checks scan direction)
  - TupIsNull (checks for null tuples)
  - outerPlanState (accesses subplan state)
- Called from (representative examples):
  - [ExecInitLimit](ExecInitLimit.md) (sets as execution function)

## Notes and Other Information
- Uses a complex state machine to handle different execution phases and edge cases
- Supports WITH TIES semantics by saving the last in-window tuple for comparison
- Handles both forward and backward scan directions with appropriate state transitions
- Position tracking is maintained across state transitions for proper offset/limit enforcement
- Error handling includes checks for subplan failures during backward scanning
- The function is designed to work with rescans while maintaining parallel execution compatibility