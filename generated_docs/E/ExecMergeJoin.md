# ExecMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:599-1443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L599-L1443)

## Overview
The core execution function that implements the merge join algorithm using a sophisticated state machine to efficiently join two pre-sorted input streams.

## Definition
```c
static TupleTableSlot *ExecMergeJoin(PlanState *pstate)
```

## Detailed Description
ExecMergeJoin implements PostgreSQL's merge join algorithm, one of the three fundamental join algorithms (along with nested loop and hash join). This function operates as a complex state machine that efficiently joins two pre-sorted input streams by advancing through both streams in a coordinated manner, taking advantage of the sort order to avoid redundant comparisons.

The algorithm maintains multiple execution states to handle different phases of the join process, including initialization, tuple comparison, skipping non-matching tuples, and handling outer join semantics. The state machine approach allows the function to be called repeatedly, returning one result tuple per call while maintaining its position in both input streams.

Key algorithmic features include:
- **State-driven execution**: Uses a comprehensive state machine with states like EXEC_MJ_INITIALIZE_OUTER, EXEC_MJ_SKIP_TEST, EXEC_MJ_JOINTUPLES
- **Mark and restore capability**: Supports backing up in the inner stream when duplicate values are found in the outer stream
- **Outer join support**: Handles LEFT, RIGHT, FULL, ANTI, and RIGHT_ANTI join types with proper null-filling logic
- **Memory efficiency**: Processes tuples one at a time without materializing entire result sets
- **Sort order dependency**: Requires both input streams to be sorted on the join keys

The function handles complex scenarios including duplicate values across join boundaries, proper outer join semantics, and various optimization flags like single_match for semi-joins.

## Parameters / Member Variables
- `pstate`: Pointer to the PlanState structure, which is cast to MergeJoinState to access merge join specific state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting)
  - innerPlanState/outerPlanState (to access child plan nodes)
  - ResetExprContext (memory management)
  - [ExecProcNode](ExecProcNode.md) (to fetch tuples from child nodes)
  - [MJEvalOuterValues](../M/MJEvalOuterValues.md)/MJEvalInnerValues (join key evaluation)
  - [MJCompare](../M/MJCompare.md) (tuple comparison)
  - [MJFillOuter](../M/MJFillOuter.md)/MJFillInner (outer join null-filling)
  - [ExecQual](ExecQual.md) (qualification testing)
  - [ExecProject](ExecProject.md) (result tuple projection)
  - [ExecMarkPos](ExecMarkPos.md)/ExecRestrPos (mark and restore operations)
  - MarkInnerTuple (marking functionality)
- Called from (representative examples):
  - [ExecInitMergeJoin](ExecInitMergeJoin.md) (sets as the execution function)

## Notes and Other Information
- Implements a complex state machine with 11 distinct execution states
- Requires input streams to be sorted on join keys; violation results in runtime error
- Supports all PostgreSQL join types including outer joins and anti-joins
- Uses mark/restore capability of inner plan when available for handling duplicate join keys
- Memory context is reset per tuple to prevent memory leaks during long-running joins
- Includes extensive debugging support through MJ_printf and MJ_dump macros
- The function can be interrupted via CHECK_FOR_INTERRUPTS() for query cancellation
- Performance depends heavily on the sort order and distribution of join keys
- State transitions are carefully designed to handle edge cases like end-of-stream conditions