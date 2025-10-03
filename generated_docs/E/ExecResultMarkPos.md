# ExecResultMarkPos

## Location
[src/backend/executor/nodeResult.c:146-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeResult.c#L146-L160)

## Overview
ExecResultMarkPos marks the current position in a Result node for potential restoration later, delegating the mark operation to its outer plan if present.

## Definition

```c
void
ExecResultMarkPos(ResultState *node)
```
## Detailed Description
ExecResultMarkPos implements the mark/restore functionality for Result plan nodes. This function is part of PostgreSQL's position marking mechanism that allows certain plan nodes to remember their current execution position and restore it later.

The function checks if the Result node has an outer plan:
- **If outer plan exists**: Delegates the mark operation to the outer plan by calling ExecMarkPos on it
- **If no outer plan**: Logs a debug message indicating that standalone Result nodes (those generating constant results) do not support mark/restore operations

This design reflects the fact that Result nodes themselves don't maintain complex execution state that needs to be marked - they either pass through tuples from an outer plan or generate a single constant result.

## Parameters / Member Variables
- `*node`: The ResultState containing the execution state for this Result node
## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (to get the outer plan state)
  - [ExecMarkPos](ExecMarkPos.md) (to mark position in the outer plan)
  - elog (to log debug messages when mark/restore is not supported)
- Called from:
  - [ExecMarkPos](ExecMarkPos.md) (general position marking dispatcher in execAmi.c)
  - Declared in nodeResult.h

## Notes and Other Information
- [Result](../R/Result.md) nodes without outer plans (constant result generators) inherently don't support mark/restore since they produce at most one tuple
- The debug message helps identify when mark/restore is being attempted on unsupported Result node configurations
- Mark/restore functionality is typically used by nested loop joins and similar operations that need to replay tuple streams

## Simplified Source

```c
void ExecResultMarkPos(ResultState *node)
{
    PlanState *outerPlan = outerPlanState(node);

    // Delegate to outer plan if present
    if (outerPlan != NULL)
        ExecMarkPos(outerPlan);
    else
        elog(DEBUG2, "Result nodes do not support mark/restore");
}
```