# ExecReadyInterpretedExpr

## Location
src/backend/executor/execExprInterp.c: 236 - 395

## Overview
Prepares an ExprState for interpreted execution by initializing the interpreter, setting up fast-path optimizations for simple expressions, and configuring appropriate evaluation functions.

## Definition
```c
void ExecReadyInterpretedExpr(ExprState *state)
```

## Detailed Description
This function serves as the initialization routine for PostgreSQL's expression interpreter. It performs several key tasks:

1. **One-time interpreter setup**: Ensures the global interpreter is initialized via ExecInitInterpreter()
2. **Validation**: Performs basic sanity checks on the expression steps
3. **Redundant initialization prevention**: Checks the EEO_FLAG_INTERPRETER_INITIALIZED flag to avoid duplicate setup
4. **Fast-path optimization**: Identifies very simple expression patterns (2-3 steps) and assigns specialized evaluation functions to avoid interpreter overhead
5. **Direct threading setup**: In builds with computed goto support, replaces opcodes with direct jump addresses for improved performance

The function implements an important optimization strategy by recognizing common simple patterns like single variable access, constants, and basic assignments, then routing them to dedicated fast-path functions rather than the full interpreter.

## Parameters / Member Variables
- `state`: Pointer to ExprState structure containing the expression to be prepared for execution

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitInterpreter](ExecInitInterpreter.md)
  - [ExecInterpExprStillValid](ExecInterpExprStillValid.md)
  - [ExecJustInnerVar](ExecJustInnerVar.md), ExecJustOuterVar, ExecJustScanVar (fast-path functions)
  - [ExecJustAssignInnerVar](ExecJustAssignInnerVar.md), ExecJustAssignOuterVar, ExecJustAssignScanVar (assignment fast-paths)
  - [ExecJustConst](ExecJustConst.md), ExecJustInnerVarVirt, ExecJustOuterVarVirt, ExecJustScanVarVirt (virtual fast-paths)
  - [ExecInterpExpr](ExecInterpExpr.md) (main interpreter function)
- Called from:
  - [ExecReadyExpr](ExecReadyExpr.md) (src/backend/executor/execExpr.c:882)

## Notes and Other Information
- The function sets state->evalfunc initially to ExecInterpExprStillValid, which performs validation on first execution before switching to the actual evaluation function
- Fast-path optimizations cover the most common expression patterns: simple variable access, constants, and assignments
- The EEO_FLAG_DIRECT_THREADED flag is used in computed goto builds to enable direct threading optimization
- This function is part of PostgreSQL's expression evaluation subsystem, which provides both interpreted and JIT-compiled execution paths