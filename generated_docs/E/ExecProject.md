# ExecProject

## Location
[src/include/executor/executor.h:377-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/executor.h#L377-L413)

## Overview
ExecProject performs tuple projection operations by evaluating a set of expressions and constructing a result tuple with the computed values in the target slot.

## Definition

```c
static inline TupleTableSlot *
ExecProject(ProjectionInfo *projInfo)
```
## Detailed Description
ExecProject is the core function for tuple projection in PostgreSQL's executor. It takes a ProjectionInfo structure containing compiled projection expressions and produces a result tuple by evaluating those expressions in the proper memory context. The function first clears the result slot to prepare it for new data, then evaluates the projection expression (which typically contains multiple target expressions compiled into a single evaluation step), and finally marks the result slot as containing a valid virtual tuple.

The projection process is highly optimized through PostgreSQL's expression compilation system, where multiple target list expressions are compiled into efficient step-based evaluation sequences. This allows the entire projection operation to be performed with minimal overhead, making it suitable for processing large numbers of tuples efficiently.

## Parameters / Member Variables
- : ProjectionInfo containing the compiled projection expressions, expression context, and result slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple (to prepare the result slot)
  - ExecEvalExprSwitchContext (to evaluate projection expressions with proper memory context)
  - TTS_FLAG_EMPTY (flag manipulation for slot status)
- Called from (representative examples):
  - ExecScan (for scan node projections)
  - project_aggregates (in aggregate processing)
  - ExecGather and ExecGatherMerge (for parallel query results)
  - ExecGroup (for grouping operations)
  - Various join implementations (HashJoin, MergeJoin, NestLoop)
  - ExecModifyTable operations (INSERT, UPDATE, MERGE projections)

## Notes and Other Information
- This is a static inline function defined in executor.h, ensuring efficient execution for this frequently-called operation
- The function implements the "virtual tuple" concept where the result slot contains references to computed values rather than a physically materialized tuple
- The projection expressions are pre-compiled during query initialization for optimal runtime performance
- The result slot's Datum/isnull arrays are used as workspace during expression evaluation, which is safe after clearing the slot
- The function handles complex projections including function calls, expressions, and simple column references uniformly through the compiled expression system
- Memory context switching ensures that any temporary allocations during projection are properly managed and cleaned up per tuple