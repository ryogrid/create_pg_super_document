# ExecJustAssignInnerVar

## Location
[src/backend/executor/execExprInterp.c:2214-2220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2214-L2220)

## Overview
ExecJustAssignInnerVar is a fast-path function for assigning values from inner tuple variables to result tuple slots in PostgreSQL's expression evaluation system.

## Definition
```c
static Datum ExecJustAssignInnerVar(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
ExecJustAssignInnerVar is a specialized wrapper function that handles the assignment of variable values from the inner tuple to the result tuple slot. This function is part of PostgreSQL's expression evaluation fast-path optimization system, specifically designed for join operations where values need to be extracted from the inner relation's tuple and assigned to output columns.

The function serves as a thin wrapper around ExecJustAssignVarImpl, configured to work with the inner tuple slot (econtext->ecxt_innertuple). This optimization is particularly important in join operations where inner tuple attributes frequently need to be projected into the result.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state and step information including result slot configuration
- `econtext`: ExprContext providing access to the inner tuple slot via ecxt_innertuple
- `isnull`: Output parameter set to indicate if the assigned value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustAssignVarImpl](ExecJustAssignVarImpl.md)
- Called from (representative examples):
  - EEO_JUMP (via expression evaluation dispatch mechanism)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (during expression setup and initialization)

## Notes and Other Information
- Part of the assignment expression fast-path family along with ExecJustAssignOuterVar and ExecJustAssignScanVar
- Specifically optimized for join operations where inner tuple attributes are projected to output
- Returns 0 like all assignment functions, with the actual result stored in the result tuple slot
- The function assumes that expression compilation has already validated the variable references and tuple slot compatibility
- Marked as static and designed for inlining to minimize function call overhead in tight execution loops