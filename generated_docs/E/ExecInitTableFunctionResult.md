# ExecInitTableFunctionResult

## Location
[src/backend/executor/execSRF.c:56-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L56-L100)

## Overview
Prepares a table function call in FROM clause (ROWS FROM syntax) for execution by initializing the necessary state structures and determining the execution strategy based on whether the function returns a set.

## Definition

```c
SetExprState *
ExecInitTableFunctionResult(Expr *expr,
							ExprContext *econtext, PlanState *parent)
```
## Detailed Description
ExecInitTableFunctionResult is responsible for setting up the execution state for table functions used in FROM clauses. The function creates a SetExprState node and configures it based on the type of expression provided. It handles two main execution paths:

1. **FuncExpr path**: When the expression is a FuncExpr (the normal case for table function references), it extracts function metadata, initializes argument expressions, and calls init_sexpr to set up the function call infrastructure.

2. **General expression path**: When the planner has transformed the function call (through constant-folding or inlining), it falls back to using the general ExecEvalExpr() mechanism via ExecInitExpr.

The function is specifically designed for nodeFunctionscan.c and handles the initialization phase of table function execution, preparing all necessary state for subsequent calls to retrieve function results.

## Parameters / Member Variables
- `*expr`: The expression tree representing the table function call (typically a FuncExpr)
- `*econtext`: The expression context providing memory context and execution environment
- `*parent`: The parent PlanState node for proper integration into the execution tree
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (SetExprState creation)
  - IsA (type checking for FuncExpr)
  - [ExecInitExprList](ExecInitExprList.md) (argument initialization)
  - [init_sexpr](../i/init_sexpr.md) (function call setup)
  - [ExecInitExpr](ExecInitExpr.md) (fallback expression initialization)
- Called from (representative examples):
  - [ExecInitFunctionScan](ExecInitFunctionScan.md) (src/backend/executor/nodeFunctionscan.c:349)

## Notes and Other Information
- The function assumes that table function references are normally FuncExpr nodes, but provides fallback handling for optimized cases
- Set-returning functions are only supported in the FuncExpr path; the general expression path cannot handle buried set-returning functions
- The state->funcReturnsSet flag is properly initialized to indicate whether the function can return multiple rows
- Uses the per-query memory context from the expression context for function state allocation

## Simplified Source

```c
SetExprState *
ExecInitTableFunctionResult(Expr *expr, ExprContext *econtext, PlanState *parent)
{
    // Create set expression state
    SetExprState *state = makeNode(SetExprState);

    // Initialize basic state
    state->funcReturnsSet = false;
    state->expr = expr;
    state->func.fn_oid = InvalidOid;

    // Handle normal function calls (FuncExpr)
    if (IsA(expr, FuncExpr)) {
        FuncExpr *func = (FuncExpr *) expr;

        // Set up function call state
        state->funcReturnsSet = func->funcretset;
        state->args = ExecInitExprList(func->args, parent);

        // Initialize function call machinery
        init_sexpr(func->funcid, func->inputcollid, expr, state, parent,
                   econtext->ecxt_per_query_memory, func->funcretset, false);
    } else {
        // Fallback for optimized expressions (constant-folded, inlined)
        state->elidedFuncState = ExecInitExpr(expr, parent);
    }

    return state;
}
```