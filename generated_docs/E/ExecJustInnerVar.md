# ExecJustInnerVar

## Location
src/backend/executor/execExprInterp.c: 2167 - 2173

## Overview
ExecJustInnerVar is a fast-path function for evaluating simple expressions that reference a variable from the inner tuple in join operations.

## Definition


## Detailed Description
This function provides optimized evaluation for expressions consisting solely of a reference to an attribute from the inner relation in a join operation. It serves as a specialized wrapper around ExecJustVarImpl, specifically targeting the inner tuple slot from the expression context.

The function is part of PostgreSQL's fast-path expression evaluation system, designed to eliminate the overhead of the general expression interpretation machinery for simple variable references. It directly accesses the inner tuple from the expression context and delegates the actual attribute retrieval to ExecJustVarImpl.

## Parameters / Member Variables
- : Pointer to ExprState containing expression evaluation steps and metadata
- : Pointer to ExprContext providing access to the execution context including tuple slots
- : Pointer to boolean that will be set to true if the retrieved attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - ExecJustVarImpl (core implementation)
- Called from (representative examples):
  - EEO_JUMP (macro expansion for expression evaluation)
  - ExecReadyInterpretedExpr (expression preparation)

## Notes and Other Information
- Located in src/backend/executor/execExprInterp.c:2167-2173
- Part of the fast-path expression evaluation system for join operations
- Specifically targets inner relation attributes in join contexts
- Provides minimal overhead for the common case of simple inner variable references
- Works in conjunction with ExecJustOuterVar and ExecJustScanVar for complete variable reference coverage