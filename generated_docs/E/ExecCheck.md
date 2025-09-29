# ExecCheck

## Location
[src/backend/executor/execExpr.c:847-876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L847-L876)

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
  - [ExecEvalExprSwitchContext](ExecEvalExprSwitchContext.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - Assert
  - EEO_FLAG_IS_QUAL
- Called from (representative examples):
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ExecRelCheck](ExecRelCheck.md)
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)
  - [domain_check_input](../d/domain_check_input.md)
  - [ExecQualAndReset](ExecQualAndReset.md)

## Notes and Other Information
- Returns true immediately if state is NULL (no constraint to check)
- NULL constraint evaluation results are treated as TRUE per SQL standard
- Includes assertion to verify expression was not compiled as a qualifier
- Used extensively for table constraints, partition constraints, and domain constraints
- The function switches execution context to ensure proper memory management during evaluation

## Simplified Source

```c
bool ExecCheck(ExprState *state, ExprContext *econtext) {
    // No constraint to check - passes by default
    if (state == NULL)
        return true;

    // Verify expression was prepared correctly
    Assert(!(state->flags & EEO_FLAG_IS_QUAL));

    // Evaluate the constraint expression
    bool isnull;
    Datum result = ExecEvalExprSwitchContext(state, econtext, &isnull);

    // NULL result means constraint passes (SQL standard)
    if (isnull)
        return true;

    // Return boolean result
    return DatumGetBool(result);
}
```