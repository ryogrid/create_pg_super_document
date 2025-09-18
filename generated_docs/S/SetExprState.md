# SetExprState

## Location
src/include/nodes/execnodes.h: 890 - 954

## Overview
SetExprState manages the execution state for set-returning expressions (SRFs), handling both true set-returning functions and expressions treated as single-row sets in ROWS FROM clauses.

## Definition
```c
typedef struct SetExprState
{
    NodeTag             type;
    Expr               *expr;              /* expression plan node */
    List               *args;              /* ExprStates for argument expressions */
    ExprState          *elidedFuncState;   /* inlined expression for ROWS FROM */
    FmgrInfo            func;              /* function manager lookup info */
    Tuplestorestate    *funcResultStore;   /* tuplestore for SRF results */
    TupleTableSlot     *funcResultSlot;    /* slot for current result row */
    TupleDesc           funcResultDesc;    /* tuple descriptor for output */
    bool                funcReturnsTuple;  /* valid when funcResultDesc isnt NULL */
    bool                funcReturnsSet;    /* function declared to return set */
    bool                setArgsValid;      /* fcinfo contains valid args for continuation */
    bool                shutdown_reg;      /* shutdown callback registered */
    FunctionCallInfo    fcinfo;            /* function call parameter structure */
} SetExprState;
```

## Detailed Description
SetExprState provides comprehensive state management for expressions that may return multiple rows, particularly set-returning functions (SRFs). This structure handles the complex lifecycle of SRF evaluation, including argument management, result storage, and proper cleanup.

The design supports multiple evaluation modes: value-per-call for functions that return one row at a time, and materialize mode where all results are stored in a tuplestore. The structure also accommodates optimized cases where functions are inlined in ROWS FROM clauses, eliminating function call overhead.

Key to its operation is the management of function call continuity through setArgsValid, which ensures that multi-call SRFs receive consistent arguments across their evaluation sequence. The integration with PostgreSQLs tuple storage and slot systems enables efficient row-by-row result delivery regardless of the underlying functions return pattern.

## Parameters / Member Variables
- `type`: NodeTag for runtime type identification as SetExprState
- `expr`: Pointer to the expression plan node (FuncExpr, OpExpr, etc.) being evaluated
- `args`: List of ExprState nodes for evaluating the functions argument expressions
- `elidedFuncState`: Compiled expression state for inlined functions in ROWS FROM that dont actually call the function
- `func`: FmgrInfo structure containing function manager lookup information and cached function metadata
- `funcResultStore`: Tuplestorestate for storing results from functions that return multiple rows at once
- `funcResultSlot`: TupleTableSlot containing the currently active result row being returned to caller
- `funcResultDesc`: Tuple descriptor describing the structure of the functions output tuples
- `funcReturnsTuple`: Boolean indicating whether the function returns composite values (valid when funcResultDesc is set)
- `funcReturnsSet`: Boolean flag indicating whether the function is declared to return a set (set during initialization)
- `setArgsValid`: Boolean indicating that fcinfo contains valid arguments for continuing a multi-call SRF sequence
- `shutdown_reg`: Boolean tracking whether a shutdown callback has been registered for proper cleanup
- `fcinfo`: FunctionCallInfo structure containing the complete function call context and arguments

## Dependencies
- Functions called/Symbols referenced:
  - Expr (expression plan nodes)
  - ExprState (argument expression states)
  - FmgrInfo (function manager info)
  - Tuplestorestate (result storage)
  - TupleTableSlot (row access interface)
  - TupleDesc (tuple structure description)
  - FunctionCallInfo (function call context)
- Called from (representative examples):
  - ExecInitTableFunctionResult (src/backend/executor/execSRF.c:59)
  - ExecMakeTableFunctionResult (src/backend/executor/execSRF.c:101)
  - ExecInitFunctionResultSet (src/backend/executor/execSRF.c:447)
  - ExecMakeFunctionResultSet (src/backend/executor/execSRF.c:497)
  - FunctionScanPerFuncState (src/backend/executor/nodeFunctionscan.c:37)
  - ExecProjectSRF (src/backend/executor/nodeProjectSet.c:177)

## Notes and Other Information
- Central to PostgreSQLs set-returning function infrastructure, handling both traditional SRFs and ROWS FROM expressions
- Supports optimization through function inlining (elidedFuncState) when functions can be compiled into direct expressions
- The setArgsValid mechanism is crucial for value-per-call SRFs, ensuring argument consistency across multiple invocations
- Integrates with the tuplestore system for efficient handling of functions that produce large result sets
- Shutdown callback registration ensures proper cleanup of resources like tuplestores when execution contexts are destroyed
- Used extensively in ProjectSet nodes for handling multiple SRFs in SELECT clause projections
- The funcReturnsSet flag is determined at initialization time and remains valid throughout execution
- Handles the complexity of PostgreSQLs dual-mode SRF calling convention (value-per-call vs. materialize)
- Memory management is carefully coordinated between function call contexts and result storage systems