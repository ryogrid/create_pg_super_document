# ExecInitWorkTableScan

## Location
[src/backend/executor/nodeWorktablescan.c:130-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWorktablescan.c#L130-L190)

## Overview
ExecInitWorkTableScan initializes a WorkTableScan plan node for execution, setting up the necessary state structures and execution context for scanning temporary worktables in recursive queries.

## Definition
WorkTableScanState *ExecInitWorkTableScan(WorkTableScan *node, EState *estate, int eflags)

## Detailed Description
ExecInitWorkTableScan performs the initialization phase for WorkTableScan plan nodes within PostgreSQL's execution framework. It creates and configures a WorkTableScanState structure with all necessary components for tuple scanning, including expression context, result tuple types, scan tuple slots, and qualification expressions. The function validates that no unsupported execution flags (backward scanning, mark/restore) are requested and ensures the plan node has no child nodes as expected. Notably, it defers projection info initialization until the actual execution phase, allowing ExecWorkTableScan to handle the complex timing dependencies with ancestor RecursiveUnion nodes. The initialization establishes minimal tuple operations for efficient tuple handling.

## Parameters / Member Variables
- `node`: WorkTableScan plan node containing the scan plan information
- `estate`: EState pointer to the execution state context  
- `eflags`: Integer bitmask of execution flags (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new WorkTableScanState node)
  - outerPlan/innerPlan (macros to check for child plans) 
  - ExecAssignExprContext (assigns expression evaluation context)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (initializes result tuple type from target list)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md) (initializes scan tuple slot)
  - [ExecInitQual](ExecInitQual.md) (initializes qualification expressions)
  - [ExecWorkTableScan](ExecWorkTableScan.md) (sets as the execution procedure)
- Types used:
  - WorkTableScan (plan node structure)
  - [WorkTableScanState](../W/WorkTableScanState.md) (execution state structure)
  - [EState](EState.md) (executor state)
  - TTSOpsMinimalTuple (tuple slot operations)
- Called from:
  - [ExecInitNode](ExecInitNode.md) (generic plan node initialization dispatcher)

## Notes and Other Information
- Does not support backward scanning or mark/restore operations for performance reasons
- Expects no child plan nodes (leaf node in execution tree)
- Defers projection info initialization to handle RecursiveUnion timing dependencies
- Sets up minimal tuple operations for efficient tuple processing
- Initializes result type as not yet fixed, allowing later type assignment
- The rustate field is set to NULL initially and populated during first execution
- Qualification expressions are fully initialized during this phase