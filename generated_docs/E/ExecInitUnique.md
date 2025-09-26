# ExecInitUnique

## Location
[src/backend/executor/nodeUnique.c:114-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeUnique.c#L114-L167)

## Overview
ExecInitUnique initializes the UniqueState execution state structure and sets up the UNIQUE plan node for execution, including initializing its outer subplan and preparing equality comparison functions.

## Definition
UniqueState *ExecInitUnique(Unique *node, EState *estate, int eflags)

## Detailed Description
ExecInitUnique performs comprehensive initialization of a UNIQUE plan node. It creates and configures a UniqueState structure, assigns the execution function pointer to ExecUnique, and initializes the expression context for evaluation. The function then recursively initializes the outer subplan and sets up the result tuple slot with minimal tuple operations for efficiency. A key aspect of initialization is precomputing the equality comparison functions using execTuplesMatchPrepare, which prepares optimized comparison routines based on the columns and operators specified in the plan. The function validates that unsupported execution flags are not set and ensures the node is properly configured for duplicate elimination.

## Parameters / Member Variables
- : Pointer to the Unique plan node containing configuration information like column indices and comparison operators
- : Execution state containing global execution context and parameters
- : Execution flags specifying special execution modes (backward scan and mark/restore are unsupported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Create new UniqueState structure
  - [ExecAssignExprContext](ExecAssignExprContext.md): Set up expression evaluation context
  - [ExecInitNode](ExecInitNode.md): Recursively initialize outer subplan
  - outerPlan: Get outer plan from Unique node
  - outerPlanState: Get outer plan state from UniqueState
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md): Initialize result tuple slot with minimal tuple ops
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md): Precompute equality comparison functions
  - [ExecGetResultType](ExecGetResultType.md): Get result tuple descriptor from outer plan
  - [ExecUnique](ExecUnique.md): Main execution function assigned to ExecProcNode
- Called from:
  - [ExecInitNode](ExecInitNode.md): During plan tree initialization
  - nodeUnique.h: Header declaration

## Notes and Other Information
- Asserts that EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not set as they are unsupported
- Sets ps_ProjInfo to NULL since UNIQUE nodes perform no projections
- Uses TTSOpsMinimalTuple for efficient tuple slot operations
- Precomputes equality functions for performance optimization during execution
- The eqfunction field stores prepared comparison routines used by ExecUnique for duplicate detection