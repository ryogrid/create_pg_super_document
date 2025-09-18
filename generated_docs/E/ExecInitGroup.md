# ExecInitGroup

## Location
src/backend/executor/nodeGroup.c: 161 - 225

## Overview
ExecInitGroup initializes the runtime state for a Group plan node, setting up the necessary data structures, expression contexts, and child node connections for GROUP BY execution.

## Definition


## Detailed Description
ExecInitGroup performs comprehensive initialization of a GroupState node during query plan startup. It creates the execution state structure, establishes parent-child relationships in the plan tree, initializes expression evaluation contexts, and sets up tuple processing infrastructure.

Key initialization tasks include:
- Creating and configuring the GroupState structure
- Initializing the child (outer) plan node recursively
- Setting up scan slots and result slots with appropriate tuple operations
- Initializing projection info for result tuple formation
- Compiling HAVING clause expressions for runtime evaluation  
- Precomputing tuple comparison functions for efficient group boundary detection

The function also validates that unsupported execution flags (backward scan, mark/restore) are not requested, as Group nodes don't support these operations.

## Parameters / Member Variables
- : The Group plan node from the planner containing grouping specification
- : The execution state containing global query execution context
- : Execution flags controlling scan behavior and optimization hints

## Dependencies
- Functions called/Symbols referenced:
  - GroupState (state structure creation)
  - Group (plan node type)
  - TupleTableSlotOps (slot operations)
  - EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK (unsupported flags)
  - makeNode (memory allocation)
  - ExecGroup (assigned as execution function)
  - ExecAssignExprContext (expression context setup)
  - ExecInitNode (child initialization)
  - outerPlan, outerPlanState (child plan access)
  - ExecGetResultSlotOps (slot operations determination)
  - ExecCreateScanSlotFromOuterPlan (scan slot creation)
  - ExecInitResultTupleSlotTL (result slot initialization)
  - ExecAssignProjectionInfo (projection setup)
  - ExecInitQual (HAVING clause compilation)
  - execTuplesMatchPrepare (group comparison setup)
  - ExecGetResultType (type information)
- Called from (representative examples):
  - ExecInitNode (during plan tree initialization)

## Notes and Other Information
- Validates that backward scanning and mark/restore are not requested
- Uses virtual tuple slots for result tuples to optimize memory usage
- Precomputes tuple comparison functions using execTuplesMatchPrepare for efficient group detection
- The eqfunction field stores compiled comparison logic for the grouping columns
- Child node initialization happens before parent node setup to establish proper dependencies
- Expression contexts are essential for qual and projection evaluation during execution