# ExecJustScanVar

## Location
src/backend/executor/execExprInterp.c: 2181 - 2187

## Overview
ExecJustScanVar is a fast-path function for executing simple variable references from the scan tuple in PostgreSQL's expression evaluation system.

## Definition


## Detailed Description
ExecJustScanVar is a specialized, optimized function designed to handle the simplest case of variable evaluation where the variable refers to a column in the scan tuple. This function is part of PostgreSQL's expression evaluation fast-path optimization system, which avoids the overhead of the general-purpose expression interpreter for trivial expressions.

The function serves as a thin wrapper around ExecJustVarImpl, specifically configured to extract values from the scan tuple (econtext->ecxt_scantuple). This optimization is particularly important for performance as variable references are extremely common in SQL queries.

## Parameters / Member Variables
- : ExprState containing the expression evaluation state and step information
- : ExprContext providing access to the various tuple slots (scan, inner, outer)
- : Output parameter set to true if the retrieved value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustVarImpl](ExecJustVarImpl.md)
  - pg_attribute_always_inline
- Called from (representative examples):
  - EEO_JUMP (via expression evaluation dispatch)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (during expression setup)

## Notes and Other Information
- This function is marked as static and is inlined for maximum performance
- Part of the expression evaluation fast-path system introduced to optimize simple expressions
- Specifically handles variables from the scan tuple, complementing ExecJustInnerVar and ExecJustOuterVar for their respective tuple sources
- Uses slot_getattr() internally through ExecJustVarImpl, which handles attribute number validation and fetching
- The function assumes that expression setup has already validated the expression structure and variable references