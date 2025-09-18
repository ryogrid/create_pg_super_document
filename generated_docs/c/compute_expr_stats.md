# compute_expr_stats

## Location
src/backend/statistics/extended_stats.c: 2090 - 2233

## Overview
Computes statistics for expression columns by evaluating expressions against sampled table rows and generating statistical summaries for use in query optimization.

## Definition


## Detailed Description
This function evaluates expressions on a sample of table rows and computes detailed statistics for each expression. It creates an executor state for expression evaluation, processes each expression against all sample rows, and generates statistical summaries including histograms, most common values, and n_distinct estimates. The function uses proper memory context management to avoid memory leaks during expression evaluation and handles null values appropriately. Statistics computed by this function are essential for the query planner to make accurate cost estimates for queries involving expressions.

## Parameters / Member Variables
- : The relation being analyzed for statistics computation
- : Total number of rows in the relation (used for statistical extrapolation)
- : Array of AnlExprData structures containing expression information and VacAttrStats objects
- : Number of expressions in the exprdata array to process
- : Array of HeapTuple pointers representing the sample rows to evaluate expressions against
- : Number of sample rows in the rows array

## Dependencies
- Functions called/Symbols referenced:
  - CreateExecutorState
  - GetPerTupleExprContext
  - ExecPrepareExpr
  - MakeSingleTupleTableSlot
  - ExecStoreHeapTuple
  - ExecEvalExprSwitchContext
  - ResetExprContext
  - ExecDropSingleTupleTableSlot
  - FreeExecutorState
  - AllocSetContextCreate
  - datumCopy
  - get_attribute_options
  - expr_fetch_func
  - MemoryContextReset
  - MemoryContextDelete
- Called from (representative examples):
  - BuildRelationExtStatistics

## Notes and Other Information
The function creates a dedicated memory context for expression evaluation to prevent memory leaks and ensure proper cleanup. Each expression is evaluated against all sample rows using PostgreSQL's expression evaluation infrastructure. The computed statistics are stored in the VacAttrStats structure and can be overridden by table-specific n_distinct options. Memory management is critical as expression evaluation can generate significant temporary data, so the function resets the per-tuple context after each row evaluation and cleans up all resources at the end.