# ExecInterpExpr

## Location
src/backend/executor/execExprInterp.c: 396 - 1914

## Overview
The core interpreter function that executes PostgreSQL expressions by evaluating a sequence of expression evaluation steps (opcodes) in a large switch statement or computed goto dispatch loop.

## Definition
```c
static Datum ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
ExecInterpExpr is PostgreSQL's expression interpreter that executes compiled expression trees represented as arrays of ExprEvalStep operations. It serves as the runtime engine for expression evaluation when JIT compilation is not available or not beneficial.

The function operates using either:
1. **Computed goto (EEO_USE_COMPUTED_GOTO)**: Uses GCC's computed goto extension for direct threading, where opcodes are replaced with jump addresses for maximum performance
2. **Traditional switch statement**: Falls back to a large switch statement for portability

Key characteristics:
- Handles over 80 different expression operation types (EEOP_* opcodes)
- Implements specialized handling for variables, constants, functions, boolean operations, type coercions, aggregates, and window functions
- Uses inline implementations for performance-critical operations
- Delegates complex operations to separate functions
- Supports conditional jumps for control flow (AND/OR short-circuiting, CASE expressions)

The interpreter processes each step sequentially, with each step potentially modifying the result value and null flag, until reaching EEOP_DONE.

## Parameters / Member Variables
- `state`: Pointer to ExprState containing the expression steps and metadata
- `econtext`: Expression context providing tuple slots and parameter values
- `isnull`: Output parameter set to true if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - Various ExecEval* functions for complex operations (ExecEvalSysVar, ExecEvalWholeRowVar, etc.)
  - [CheckOpSlotCompatibility](../C/CheckOpSlotCompatibility.md) for slot validation
  - slot_getsomeattrs for tuple attribute fetching
  - FunctionCallInvoke for user-defined function calls
  - MakeExpandedObjectReadOnlyInternal for object lifecycle management
- Called from:
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (as evalfunc_private)
  - [ExecInitInterpreter](ExecInitInterpreter.md) (for dispatch table initialization)

## Notes and Other Information
- The function contains a special case: when state is NULL, it returns the dispatch table address for computed goto initialization
- Performance is critical as this is in the hot path for all expression evaluation
- The large switch statement is organized by operation complexity, with simple operations like variable access inlined
- Supports both strict and non-strict function evaluation with NULL handling
- Implements SQL three-valued logic for boolean operations (TRUE/FALSE/NULL)
- The computed goto optimization can provide significant performance improvements on supported compilers