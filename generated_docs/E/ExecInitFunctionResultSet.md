# ExecInitFunctionResultSet

## Location
src/backend/executor/execSRF.c: 444 - 496

## Overview
Prepares a set-returning function (SRF) call in the target list for execution, specifically designed for nodeProjectSet.c to handle functions that return multiple rows.

## Definition
```c
SetExprState *ExecInitFunctionResultSet(Expr *expr, ExprContext *econtext, PlanState *parent)
```

## Detailed Description
ExecInitFunctionResultSet is specialized for initializing set-returning functions that appear in the SELECT target list (as opposed to FROM clauses). It creates and configures a SetExprState node specifically for functions that are known to return sets. The function handles two main expression types:

1. **FuncExpr**: Regular function calls like `generate_series(1,10)`
2. **OpExpr**: Operator expressions that resolve to set-returning functions

Unlike ExecInitTableFunctionResult, this function assumes the expression will always be a set-returning function and sets `funcReturnsSet = true` from the start. It calls init_sexpr with both the `funcReturnsSet` and `needDescForSRF` parameters set to true, indicating special handling is needed for set-returning function result descriptors.

The function includes an assertion to verify that the resolved function actually does return a set, catching configuration errors during development.

## Parameters / Member Variables
- `expr`: The expression tree representing the set-returning function call (FuncExpr or OpExpr)
- `econtext`: Expression context providing memory context and execution environment  
- `parent`: Parent PlanState node for proper integration into the execution tree

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (SetExprState creation)
  - IsA (type checking for FuncExpr and OpExpr)
  - [ExecInitExprList](ExecInitExprList.md) (argument initialization for both function and operator expressions)
  - [init_sexpr](../i/init_sexpr.md) (function call setup with SRF-specific parameters)
  - nodeTag, elog (error handling for unsupported expression types)
- Called from (representative examples):
  - [ExecInitProjectSet](ExecInitProjectSet.md) (src/backend/executor/nodeProjectSet.c:291)

## Notes and Other Information
- Specifically designed for target list SRFs, complementing ExecInitTableFunctionResult which handles FROM clause functions
- Always sets funcReturnsSet=true unlike the table function variant which determines this dynamically
- Supports both function calls (FuncExpr) and operator calls (OpExpr) that resolve to set-returning functions
- The needDescForSRF parameter passed to init_sexpr enables special tuple descriptor handling for SRF results
- Includes runtime assertion to catch cases where non-set-returning functions are incorrectly processed through this path
- Used by nodeProjectSet.c which implements the ProjectSet plan node for handling multiple SRFs in the target list