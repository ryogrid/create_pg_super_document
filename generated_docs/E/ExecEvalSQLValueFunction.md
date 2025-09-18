# ExecEvalSQLValueFunction

## Location
[src/backend/executor/execExprInterp.c:2639-2705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2639-L2705)

## Overview
ExecEvalSQLValueFunction evaluates SQL value functions that return context-dependent values like current date/time, current user, and current schema information.

## Definition
```c
void ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function implements the evaluation of SQL value functions, which are special built-in functions that return values dependent on the current execution context. These functions include temporal functions (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP) and session information functions (CURRENT_USER, SESSION_USER, CURRENT_SCHEMA, etc.).

The function uses a switch statement to handle different SQLValueFunction operation types (SVFOP_*). For temporal functions, it calls appropriate GetSQL* functions to retrieve time values with proper precision handling via typmod. For session/context functions, it initializes function call info and invokes the corresponding system functions, properly handling potential NULL returns (particularly for current_schema which can legitimately return NULL).

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state
- `op`: ExprEvalStep containing the SQL value function operation details and result storage

## Dependencies
- Functions called/Symbols referenced:
  - SQLValueFunction
  - LOCAL_FCINFO
  - [GetSQLCurrentDate](../G/GetSQLCurrentDate.md), GetSQLCurrentTime, GetSQLCurrentTimestamp
  - [GetSQLLocalTime](../G/GetSQLLocalTime.md), GetSQLLocalTimestamp
  - [current_user](../c/current_user.md), session_user, current_database, current_schema
  - InitFunctionCallInfoData
  - Various ADT conversion functions (DateADTGetDatum, TimeTzADTPGetDatum, etc.)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Handles all standard SQL value functions in a single implementation
- Properly manages typmod for precision in temporal functions
- Some functions like current_schema() can return NULL legitimately
- Uses LOCAL_FCINFO macro for efficient function call setup
- Part of PostgreSQL's expression evaluation interpreter framework
- Maps SVFOP_* operation codes to appropriate system function calls
- Ensures proper NULL handling for each function type
- Located in src/backend/executor/execExprInterp.c:2639-2705