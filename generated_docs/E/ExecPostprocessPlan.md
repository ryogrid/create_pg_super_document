# ExecPostprocessPlan

## Location
[src/backend/executor/execMain.c:1431-1476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1431-L1476)

## Overview
Provides plan nodes a final execution opportunity before shutdown, ensuring auxiliary ModifyTable nodes run to completion for predictable results regardless of main query fetch behavior.

## Definition

```c
static void
ExecPostprocessPlan(EState *estate)
```
## Detailed Description
ExecPostprocessPlan performs final cleanup and completion tasks during executor shutdown. It specifically handles auxiliary ModifyTable nodes (stored in es_auxmodifytables) by running them to completion, ensuring all planned modifications are executed even if the main query didn't fetch all tuples. This is crucial for maintaining data consistency and predictable side effects, particularly when ModifyTable operations have been set up as auxiliary operations that might not be driven by the main query's tuple consumption.

## Parameters / Member Variables
- `*estate`: The execution state containing auxiliary ModifyTable nodes and execution context
## Dependencies
- Functions called/Symbols referenced:
  - ForwardScanDirection (constant)
  - ResetPerTupleExprContext
  - [ExecProcNode](ExecProcNode.md)  
  - TupIsNull
- Called from (representative examples):
  - [standard_ExecutorFinish](../s/standard_ExecutorFinish.md) (execMain.c:433)

## Notes and Other Information
- Forces forward scan direction to ensure consistent execution behavior
- Iterates through all auxiliary ModifyTable nodes in es_auxmodifytables list
- Runs each auxiliary node until it returns NULL (completion signal)  
- Resets per-tuple expression context between each tuple for proper memory management
- Essential for ensuring side effects occur predictably regardless of main query behavior
- Part of the executor's cleanup phase, called during ExecutorFinish
- Handles cases where auxiliary operations need completion independent of main query results
- Maintains transactional consistency by ensuring all planned modifications complete

## Simplified Source

```c
static void
ExecPostprocessPlan(EState *estate)
{
    // Ensure forward scan direction
    estate->es_direction = ForwardScanDirection;

    // Run auxiliary ModifyTable nodes to completion
    foreach(ListCell *lc, estate->es_auxmodifytables) {
        PlanState *ps = (PlanState *) lfirst(lc);

        for (;;) {
            // Reset per-tuple expression context
            ResetPerTupleExprContext(estate);

            TupleTableSlot *slot = ExecProcNode(ps);
            if (TupIsNull(slot))
                break;  // Node finished
        }
    }
}
```