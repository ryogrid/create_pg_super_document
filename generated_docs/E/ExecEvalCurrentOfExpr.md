# ExecEvalCurrentOfExpr

## Location
src/backend/executor/execExprInterp.c: 2706 - 2716

## Overview
ExecEvalCurrentOfExpr is an error-generating function that should never be executed during normal operation, as CURRENT OF expressions should be converted to other forms during planning.

## Definition
```c
void ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function serves as a fallback error handler for CURRENT OF expressions that reach the execution phase. Under normal circumstances, the PostgreSQL planner should convert CURRENT OF expressions into TidScan qualifications or handle them specially in ForeignScan nodes, so this function should never actually be called during query execution.

The function's purpose is to provide a clear error message when a CURRENT OF expression somehow makes it to the execution phase without being properly transformed. This typically indicates that a foreign table's Foreign Data Wrapper (FDW) doesn't properly support CURRENT OF operations, or there's an issue with the planning process.

The function immediately raises an error with a descriptive message indicating that WHERE CURRENT OF is not supported for the table type being accessed.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state (unused in this implementation)
- `op`: ExprEvalStep containing the operation details (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep
  - ereport (error reporting)
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (via JIT compilation)

## Notes and Other Information
- This is an intentional error-generating function, not a bug
- CURRENT OF should be handled during planning, not execution
- Typically encountered with foreign tables whose FDW lacks CURRENT OF support
- Part of PostgreSQL's expression evaluation interpreter framework
- Provides clear error messages for unsupported table types
- Located in src/backend/executor/execExprInterp.c:2706-2716
- The comment explains the expected planning behavior for CURRENT OF expressions