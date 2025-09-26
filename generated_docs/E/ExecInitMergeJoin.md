# ExecInitMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:1444-1640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L1444-L1640)

## Overview
Initializes a MergeJoinState node by setting up all necessary data structures, expression contexts, child nodes, and join-specific configuration for merge join execution.

## Definition
```c
MergeJoinState *ExecInitMergeJoin(MergeJoin *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitMergeJoin performs comprehensive initialization of a merge join node, transforming a plan node into an executable state structure. This function is responsible for setting up all the complex machinery required for merge join execution, including multiple expression contexts, child node initialization, null tuple handling for outer joins, and merge clause preprocessing.

The initialization process involves several critical steps:
- **State structure creation**: Allocates and initializes the MergeJoinState structure with proper linkage to the plan and estate
- **Expression context management**: Creates three expression contexts - the main context and two additional contexts for evaluating join expressions from left and right input tuples
- **Child node initialization**: Recursively initializes both outer and inner child plans, with special handling for mark/restore capabilities
- **Join type configuration**: Sets up appropriate flags and null tuple slots based on the specific join type (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
- **Merge clause preprocessing**: Analyzes and prepares merge clauses with operator families, collations, and comparison strategies
- **Optimization detection**: Determines whether mark/restore operations can be skipped and whether extra marks are beneficial

The function also performs important validation, ensuring that right joins and full joins only use merge-joinable conditions and that unsupported execution flags are not specified.

## Parameters / Member Variables
- `node`: Pointer to the MergeJoin plan node containing the join configuration, merge clauses, and child plan references
- `estate`: Pointer to the execution state containing global execution context, memory management, and parameter information
- `eflags`: Execution flags controlling behavior; EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are explicitly not supported

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (memory allocation)
  - ExecAssignExprContext (expression context setup)
  - CreateExprContext (additional context creation)
  - ExecInitNode (child node initialization)
  - ExecGetResultType/ExecGetResultSlotOps (type information)
  - ExecInitResultTupleSlotTL (result slot setup)
  - ExecAssignProjectionInfo (projection configuration)
  - ExecInitExtraTupleSlot (marked tuple slot)
  - ExecInitNullTupleSlot (outer join null handling)
  - ExecInitQual (qualification expression setup)
  - MJExamineQuals (merge clause preprocessing)
  - check_constant_qual (join qualification validation)
- Called from (representative examples):
  - ExecInitNode (executor node initialization dispatcher)

## Notes and Other Information
- Returns a fully initialized MergeJoinState ready for execution
- Validates that right and full joins use only merge-joinable conditions
- Sets up mark/restore optimization based on inner plan type and execution flags
- Creates null tuple slots only for join types that require them (outer joins)
- The mj_ExtraMarks optimization is enabled only for Material nodes without REWIND flag
- IndexScan and IndexOnlyScan explicitly cannot use extra marks due to positioning limitations
- Merge clauses are preprocessed to extract comparison operators, strategies, and sort information
- Initial join state is always set to EXEC_MJ_INITIALIZE_OUTER to begin execution
- Supports all PostgreSQL join types with appropriate semantic configuration