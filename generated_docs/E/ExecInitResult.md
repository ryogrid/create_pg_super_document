# ExecInitResult

## Location
src/backend/executor/nodeResult.c: 180 - 239

## Overview
ExecInitResult creates and initializes the execution state for a Result plan node, setting up all necessary data structures, child nodes, and expression evaluation contexts.

## Definition


## Detailed Description
ExecInitResult is the initialization function for Result plan nodes that creates the runtime execution state (ResultState) and prepares the node for execution. This function is part of PostgreSQL's executor initialization phase.

Key initialization steps performed:
1. **Flag validation**: Ensures that mark/restore flags are only set when an outer plan exists
2. **State creation**: Allocates and initializes a ResultState structure
3. **Execution setup**: Assigns ExecResult as the execution function
4. **Context creation**: Sets up expression evaluation context
5. **Child initialization**: Recursively initializes any outer plan child nodes
6. **Projection setup**: Initializes result tuple slots and projection information
7. **Expression compilation**: Compiles qualification expressions for efficient evaluation

The function handles both Result nodes with outer plans (filter/projection nodes) and standalone Result nodes (constant expression evaluators). It ensures that inner plans are not present (Result nodes never have right children) and properly sets up constant qualification checking flags.

## Parameters / Member Variables
- : The Result plan node from the query plan tree
- : The execution state containing global executor information
- : Execution flags indicating required capabilities (e.g., EXEC_FLAG_MARK for position marking)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ResultState structure)
  - outerPlan/innerPlan (macros to access plan tree structure) 
  - ExecAssignExprContext (to create expression evaluation context)
  - [ExecInitNode](ExecInitNode.md) (to recursively initialize child nodes)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (to initialize result tuple slot - see related processed symbols)
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md) (to set up projection infrastructure - see related processed symbols)
  - [ExecInitQual](ExecInitQual.md) (to compile qualification expressions)
- Called from:
  - [ExecInitNode](ExecInitNode.md) (the main node initialization dispatcher in execProcnode.c)
  - Declared in nodeResult.h

## Notes and Other Information
- Assert statements ensure that mark/restore functionality is only requested when an outer plan exists
- The rs_checkqual flag optimizes constant qualification evaluation by tracking whether it has been checked
- [Result](../R/Result.md) nodes never have inner (right) child plans, as verified by the assertion
- Uses virtual tuple table slot operations (TTSOpsVirtual) for efficient tuple handling
- The function integrates with PostgreSQL's expression evaluation framework for both regular and constant qualifications
- Returns a fully initialized ResultState ready for execution by ExecResult