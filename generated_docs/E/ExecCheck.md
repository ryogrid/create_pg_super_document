# ExecCheck

## Location
src/backend/executor/execExpr.c: 847 - 876

## Overview
ExecCheck evaluates a check constraint expression and returns a boolean result, treating NULL values as TRUE (constraint passes).

## Definition
bool ExecCheck(ExprState *state, ExprContext *econtext)

## Detailed Description
ExecCheck is used to evaluate check constraints in PostgreSQL. The function takes a prepared expression state and an execution context, then evaluates the constraint expression. A key feature of this function is its handling of NULL results - following SQL semantics, a NULL result from a check constraint is interpreted as TRUE, meaning the constraint passes. The function includes validation to ensure the expression was not compiled as a qualifier (using ExecInitQual), and handles the special case where no constraint exists (state == NULL) by returning true.

## Parameters / Member Variables
- state: An ExprState representing the prepared check constraint expression (NULL if no constraint)
- econtext: The expression context providing variable values and execution environment

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExprSwitchContext
  - DatumGetBool
  - Assert
  - EEO_FLAG_IS_QUAL
- Called from (representative examples):
  - ATRewriteTable
  - ExecRelCheck
  - ExecPartitionCheck
  - check_default_partition_contents
  - domain_check_input
  - ExecQualAndReset

## Notes and Other Information
- Returns true immediately if state is NULL (no constraint to check)
- NULL constraint evaluation results are treated as TRUE per SQL standard
- Includes assertion to verify expression was not compiled as a qualifier
- Used extensively for table constraints, partition constraints, and domain constraints
- The function switches execution context to ensure proper memory management during evaluation