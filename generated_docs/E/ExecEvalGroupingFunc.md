# ExecEvalGroupingFunc

## Location
src/backend/executor/execExprInterp.c: 4689 - 4715

## Overview
ExecEvalGroupingFunc implements the SQL GROUPING function by computing a bitmask that indicates which expressions are not part of the current grouping set in aggregate operations.

## Definition
void ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op)

## Detailed Description
This function evaluates the SQL GROUPING function, which is used in queries with GROUP BY GROUPING SETS, CUBE, or ROLLUP clauses. It computes a bitmask where each bit corresponds to one of the function's argument expressions. A bit is set to 1 if the corresponding expression is NOT part of the set of grouping expressions in the current grouping set, and 0 if it is part of the grouping set.

The function iterates through the clauses (column attribute numbers) associated with the GROUPING function call and checks each against the aggstate->grouped_cols bitmapset to determine if that column is being grouped in the current grouping set. The result is built as a bitmask with the rightmost bit representing the last argument.

For example, in GROUP BY GROUPING SETS ((a,b), (a), ()), calling GROUPING(a,b) would return:
- 0 (binary 00) when grouping by (a,b) - both columns are grouped
- 2 (binary 10) when grouping by (a) - only b is not grouped  
- 3 (binary 11) when grouping by () - neither column is grouped

## Parameters / Member Variables
- : The ExprState containing the expression evaluation context, with parent pointing to the AggState
- : The ExprEvalStep operation descriptor containing the grouping_func clauses list and result storage pointers

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - lfirst_int
  - bms_is_member
  - Int32GetDatum
- Called from (representative examples):
  - ExecInterpExpr (main expression interpreter loop)

## Notes and Other Information
- This function is only valid within aggregate execution contexts (AggState)
- The bitmask result follows SQL standard semantics for the GROUPING function
- Each bit position corresponds to the argument position in the GROUPING function call
- Essential for implementing SQL:1999 OLAP extensions like GROUPING SETS
- The function assumes the parent node is always an AggState when called