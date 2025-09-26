# partkey_datum_from_expr

## Location
[src/backend/partitioning/partprune.c:3760-3792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L3760-L3792)

## Overview
Evaluates an expression to extract a Datum value and null flag for use in partition pruning operations.

## Definition
```c
static void
partkey_datum_from_expr(PartitionPruneContext *context,
                       Expr *expr, int stateidx,
                       Datum *value, bool *isnull)
```

## Detailed Description
This function evaluates expressions that represent partition key values during the partition pruning process. It handles two main cases:

1. **Const expressions**: For constant expressions, it directly extracts the constant value and null flag without needing expression evaluation.

2. **Non-constant expressions**: For more complex expressions, it uses the PostgreSQL expression evaluation infrastructure with the provided ExprContext to compute the value at runtime.

The function ensures that non-constant expressions are only evaluated when a valid ExprContext is available. It uses ExecEvalExprSwitchContext to evaluate expressions in the proper memory context, which may result in memory allocation in the per-tuple context that needs to be cleaned up later.

## Parameters / Member Variables
- `context`: PartitionPruneContext containing execution context and expression states
- `expr`: The expression to be evaluated
- `stateidx`: Index into the context's exprstates array for non-constant expressions
- `value`: Output parameter to receive the evaluated Datum value
- `isnull`: Output parameter to receive the null flag of the evaluated value

## Dependencies
- Functions called/Symbols referenced:
  - IsA
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - Assert
- Called from (representative examples):
  - [perform_pruning_base_step](perform_pruning_base_step.md)

## Notes and Other Information
- Memory allocated during expression evaluation may be in the per-tuple memory context and requires cleanup via ExprContext reset
- The function asserts that either planstate or exprcontext is non-NULL when evaluating non-constant expressions
- When planstate is valid, exprcontext must be the same as planstate->ps_ExprContext
- [Const](../C/Const.md) expressions are handled efficiently without expression evaluation overhead
- Located in src/backend/partitioning/partprune.c:3760-3792