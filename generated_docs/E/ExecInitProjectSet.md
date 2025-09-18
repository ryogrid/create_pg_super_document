# ExecInitProjectSet

## Location
src/backend/executor/nodeProjectSet.c: 227 - 327

## Overview
ExecInitProjectSet creates the runtime state information for ProjectSet nodes and initializes all necessary data structures for SRF execution.

## Definition
```c
ProjectSetState *ExecInitProjectSet(ProjectSet *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitProjectSet performs comprehensive initialization of a ProjectSet node, including:

1. **State Structure Creation**: Allocates and initializes ProjectSetState with proper plan state setup
2. **Expression Compilation**: Builds evaluation state for each target list element, distinguishing between SRFs and regular expressions
3. **Memory Context Setup**: Creates a specialized memory context (`argcontext`) for SRF function arguments with appropriate lifetime management
4. **Child Node Initialization**: Initializes the outer child node (ProjectSet does not use inner plans)
5. **Tuple Slot Setup**: Configures result tuple slots using virtual tuple table slot operations

The function includes specialized logic for detecting and properly initializing set-returning functions (FuncExpr with funcretset=true and OpExpr with opretset=true) vs. regular expressions.

## Parameters / Member Variables
- `node`: The ProjectSet plan node containing the target list and plan information
- `estate`: The execution state containing query-level context and resources
- `eflags`: Execution flags controlling plan behavior (EXEC_FLAG_MARK and EXEC_FLAG_BACKWARD are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - ExecInitNode
  - ExecInitResultTupleSlotTL
  - ExecInitFunctionResultSet
  - ExecInitExpr
  - AllocSetContextCreate
  - expression_returns_set
- Called from (representative examples):
  - ExecInitNode (as part of plan tree initialization)

## Notes and Other Information
- Asserts that unsupported execution flags (MARK/BACKWARD) are not set
- Creates separate expression state for SRFs vs. regular expressions using different initialization functions
- The argcontext memory context has ALLOCSET_DEFAULT_SIZES and is specifically for tSRF function arguments
- Does not support any qualification conditions (qual must be NIL)
- Returns a fully initialized ProjectSetState ready for execution