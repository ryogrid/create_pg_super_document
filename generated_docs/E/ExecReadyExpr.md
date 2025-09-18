# ExecReadyExpr

## Location
src/backend/executor/execExpr.c: 877 - 893

## Overview
ExecReadyExpr prepares a compiled expression for execution by attempting JIT compilation first, then falling back to interpreted execution if JIT is unavailable.

## Definition
static void ExecReadyExpr(ExprState *state)

## Detailed Description
ExecReadyExpr is a crucial function in PostgreSQL's expression evaluation pipeline that finalizes the preparation of an ExprState for execution. The function first attempts to compile the expression using Just-In-Time (JIT) compilation via jit_compile_expr(). If JIT compilation succeeds, the function returns early. Otherwise, it falls back to preparing the expression for interpreted execution using ExecReadyInterpretedExpr(). This function serves as an abstraction layer that may be extended in the future to support additional expression evaluation methods beyond JIT and interpretation.

## Parameters / Member Variables
- state: An ExprState structure containing the compiled expression that needs to be prepared for execution

## Dependencies
- Functions called/Symbols referenced:
  - jit_compile_expr
  - ExecReadyInterpretedExpr
- Called from (representative examples):
  - ExecInitExpr
  - ExecInitExprWithParams
  - ExecInitQual
  - ExecBuildProjectionInfo
  - ExecBuildUpdateProjection
  - ExecInitExprRec
  - ExecBuildAggTrans
  - ExecBuildGroupingEqual
  - ExecBuildParamSetEqual

## Notes and Other Information
- This is a static function internal to execExpr.c
- Implements a two-tier execution strategy: JIT compilation preferred, interpreted execution as fallback
- Must be called for every ExprState before it can be executed
- The function is designed for future extensibility to support additional expression evaluation methods
- JIT compilation can significantly improve performance for complex expressions
- The function provides a clean abstraction that hides the choice between JIT and interpreted execution from callers