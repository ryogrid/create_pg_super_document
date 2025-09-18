# ExecEndPlan

## Location
[src/backend/executor/execMain.c:1477-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1477-L1515)

## Overview
ExecEndPlan is a static function responsible for cleaning up the query plan by closing files and freeing up storage after query execution completes.

## Definition
```c
static void ExecEndPlan(PlanState *planstate, EState *estate)
```

## Detailed Description
ExecEndPlan performs the essential cleanup operations at the end of query execution. While it's no longer primarily focused on freeing storage per se (as FreeExecutorState handles most memory release), it concentrates on critical resource cleanup including closing relations and dropping buffer pins. The function ensures that all executor resources are properly released, particularly focusing on tuple tables which must be cleared to ensure buffer pins are released.

The function operates in a systematic manner:
1. Shuts down node-type-specific query processing for the main plan
2. Recursively shuts down all subplan states
3. Destroys the executor's tuple table (focusing on buffer pins and tuple descriptor reference counts)
4. Closes any relations opened for range table entries or result relations

## Parameters / Member Variables
- `planstate`: Pointer to the top-level PlanState node representing the query execution tree that needs to be shut down
- `estate`: Pointer to the EState (execution state) containing executor-wide information including subplan states, tuple table, and relation references

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (called for main planstate and each subplanstate)
  - [ExecResetTupleTable](ExecResetTupleTable.md) (clears tuple table and releases buffer pins)
  - [ExecCloseResultRelations](ExecCloseResultRelations.md) (closes result relations)
  - [ExecCloseRangeTableRelations](ExecCloseRangeTableRelations.md) (closes range table relations)
- Called from:
  - [standard_ExecutorEnd](../s/standard_ExecutorEnd.md) (main caller for standard executor cleanup)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execMain.c file
- The function prioritizes resource cleanup over memory deallocation, relying on FreeExecutorState for comprehensive memory management
- Critical for preventing resource leaks, particularly buffer pins and relation references
- The recursive handling of subplans ensures complete cleanup of complex query trees
- Part of the standard PostgreSQL executor shutdown sequence