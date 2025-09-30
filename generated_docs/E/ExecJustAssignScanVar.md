# ExecJustAssignScanVar

## Location
[src/backend/executor/execExprInterp.c:2228-2234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2228-L2234)

## Overview
ExecJustAssignScanVar is a fast-path function for assigning values from scan tuple variables to result tuple slots in PostgreSQL's expression evaluation system.

## Definition
```c
static Datum ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
ExecJustAssignScanVar is a specialized wrapper function that handles the assignment of variable values from the scan tuple to the result tuple slot. This function is part of PostgreSQL's expression evaluation fast-path optimization system, specifically designed for operations where values need to be extracted from the current scan tuple and assigned to output columns.

The function serves as a thin wrapper around ExecJustAssignVarImpl, configured to work with the scan tuple slot (econtext->ecxt_scantuple). This optimization is particularly important in base table scans, index scans, and other operations where the scan tuple provides the source data that needs to be projected into the result.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state and step information including result slot configuration
- `econtext`: ExprContext providing access to the scan tuple slot via ecxt_scantuple
- `isnull`: Output parameter set to indicate if the assigned value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustAssignVarImpl](ExecJustAssignVarImpl.md)
- Called from (representative examples):
  - EEO_JUMP (via expression evaluation dispatch mechanism)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (during expression setup and initialization)

## Notes and Other Information
- Part of the assignment expression fast-path family along with ExecJustAssignInnerVar and ExecJustAssignOuterVar
- Specifically optimized for scan operations where scan tuple attributes are projected to output
- Returns 0 like all assignment functions, with the actual result stored in the result tuple slot
- The function assumes that expression compilation has already validated the variable references and tuple slot compatibility
- Marked as static and designed for inlining to minimize function call overhead in tight execution loops
- Particularly important for sequential scans, index scans, and other scan operations where the base table data is being projected
- Complements ExecJustScanVar which extracts values rather than assigning them to result slots

## Simplified Source

```c
static Datum
ExecJustAssignScanVar(ExprState *state, ExprContext *econtext, bool *isnull)
{
    // Assign scan tuple variable to result slot
    return ExecJustAssignVarImpl(state, econtext->ecxt_scantuple, isnull);
}
```