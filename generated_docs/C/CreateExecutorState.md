# CreateExecutorState

## Location
[src/backend/executor/execUtils.c:88-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L88-L188)

## Overview
Creates and initializes an EState node, which serves as the root of working storage for an entire Executor invocation, including the per-query memory context.

## Definition

```c
structure
	 */
	estate->es_direction = ForwardScanDirection;
```
## Detailed Description
CreateExecutorState is responsible for creating and initializing the central executor state structure (EState) that manages all execution-related data for a query. The function creates a per-query memory context named "ExecutorState" as a child of the current memory context, which will hold all working data that persists for the duration of the query execution. The EState node itself is allocated within this per-query context to avoid requiring a separate cleanup operation at shutdown.

The function initializes all fields of the EState structure to their default values, including scan direction, snapshots, range tables, result relations, parameter information, expression contexts, and various execution flags. This provides a clean, consistent starting state for query execution.

## Parameters / Member Variables
This function takes no parameters and returns a fully initialized EState pointer.

Key EState fields initialized:
- : Set to ForwardScanDirection for default forward scanning
- : Set to InvalidSnapshot (caller must initialize)
- : Set to InvalidSnapshot (no crosscheck initially)
- : Initialized to NIL (empty list)
- : Set to the newly created per-query memory context
- : Set to 0 (no tuples processed yet)
- : Initialized to NIL (empty list)
- : Set to NULL (no JIT compilation initially)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES
  - ForwardScanDirection
  - InvalidSnapshot
  - CommandId
  - makeNode
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)

- Called from (representative examples):
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md)
  - [EvalPlanQualStart](../E/EvalPlanQualStart.md)
  - [evaluate_expr](../e/evaluate_expr.md)
  - [CopyFrom](CopyFrom.md)
  - [compute_index_stats](../c/compute_index_stats.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md)

## Notes and Other Information
The function creates a memory context hierarchy where the ExecutorState context becomes a child of the current memory context. This design ensures proper memory management and cleanup when the query execution completes. The caller is responsible for initializing the es_snapshot field after calling this function, as it's left as InvalidSnapshot by design. The function is central to PostgreSQL's executor architecture and is called at the beginning of most query execution paths.