# ExecInitLimit

## Location
src/backend/executor/nodeLimit.c: 447 - 533

## Overview
ExecInitLimit initializes a Limit node's state structure and subplan, setting up expression evaluation, result type information, and WITH TIES comparison functionality if needed.

## Definition


## Detailed Description
ExecInitLimit is responsible for setting up all necessary state and data structures for a Limit execution node. The function creates a LimitState structure, initializes the child plan, sets up expression contexts for evaluating LIMIT/OFFSET parameters, and configures result type information.

Key initialization tasks include:
- Creating the LimitState node and linking it to the plan tree
- Setting the execution function pointer to ExecLimit
- Initializing the outer (child) plan node
- Setting up expression evaluation for LIMIT and OFFSET parameters
- Configuring result tuple slot operations to match the child plan
- For WITH TIES: creating comparison infrastructure including an extra tuple slot for storing the boundary tuple and setting up equality functions for tie detection

The function ensures compatibility with the execution framework by properly initializing expression contexts, result type information, and plan state linkage.

## Parameters / Member Variables
- : Limit plan node containing the configuration and expressions
- : Executor state containing global execution context
- : Execution flags (EXEC_FLAG_MARK is not supported and asserted against)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates LimitState structure)
  - ExecAssignExprContext (sets up expression evaluation context)  
  - ExecInitNode (initializes child plan recursively)
  - ExecInitExpr (initializes LIMIT/OFFSET expressions)
  - ExecInitResultTypeTL (sets up result tuple descriptor)
  - ExecGetResultSlotOps (gets tuple slot operations)
  - ExecGetResultType (gets child plan's result type)
  - ExecInitExtraTupleSlot (creates slot for WITH TIES boundary tuple)
  - execTuplesMatchPrepare (prepares tuple comparison for WITH TIES)
- Called from (representative examples):
  - ExecInitNode (as part of plan tree initialization)

## Notes and Other Information
- Does not support EXEC_FLAG_MARK execution flag (would be used for mark/restore functionality)
- Sets ps_ProjInfo to NULL since Limit nodes perform no projection
- WITH TIES support requires additional initialization of comparison infrastructure
- The limit/offset expressions are not evaluated during initialization since parameters may not be available yet
- Result slot operations are inherited from the child plan for efficiency
- Expression context is required even though Limit nodes don't use ExecQual or ExecProject