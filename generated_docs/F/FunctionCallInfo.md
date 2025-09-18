# FunctionCallInfo

## Location
src/include/fmgr.h: 38 - 55

## Overview
FunctionCallInfo is a typedef representing a pointer to FunctionCallInfoBaseData, serving as the primary interface for passing function call context and arguments to PostgreSQL functions.

## Definition


## Detailed Description
FunctionCallInfo is the fundamental type used throughout PostgreSQL's function manager system to pass function call information between the system and user-defined functions. It points to a FunctionCallInfoBaseData structure that contains all necessary context for function execution, including function arguments, null flags, result information, and execution context. This design allows PostgreSQL functions to receive a standardized interface regardless of how they are called (directly, through SQL, via triggers, etc.). All functions that can be called directly by the function manager must accept a FunctionCallInfo parameter, making it the universal function signature for PostgreSQL's pluggable function system.

## Parameters / Member Variables
- This is a typedef pointing to FunctionCallInfoBaseData structure which contains:
  - Function arguments and their null indicators
  - Result value and null flag
  - Function manager information
  - Memory context and other execution state

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCallInfoBaseData](FunctionCallInfoBaseData.md) (struct - the actual data structure)
  - PGFunction (uses FunctionCallInfo as parameter type)
- Called from (representative examples):
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [ExecInterpExpr](../E/ExecInterpExpr.md) (expression interpretation)
  - [ExecEvalFuncExprFusage](../E/ExecEvalFuncExprFusage.md) (function expression evaluation)
  - [advance_transition_function](../a/advance_transition_function.md) (aggregate function processing)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (aggregate context checking)
  - Various PL/* language handlers (plperl, plpython, pltcl)
  - JSON/JSONB processing functions
  - Array and range type operations
  - Window functions and ordered set aggregates

## Notes and Other Information
- This is the universal function call interface for all PostgreSQL functions
- All functions callable by fmgr must have the signature: Datum function_name(FunctionCallInfo fcinfo)
- Used extensively in executor, function manager, aggregate processing, and procedural languages
- Provides access to function arguments through PG_GETARG_* macros
- Enables result setting through PG_RETURN_* macros
- Central to PostgreSQL's extensibility architecture
- Supports both strict and non-strict function calling conventions
- Used in both built-in and user-defined functions
- Essential for aggregate functions, window functions, and set-returning functions