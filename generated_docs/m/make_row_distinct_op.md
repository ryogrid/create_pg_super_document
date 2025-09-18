# make_row_distinct_op

## Location
[src/backend/parser/parse_expr.c:3018-3061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3018-L3061)

## Overview
Transforms a "row IS DISTINCT FROM row" construct by creating pairwise DISTINCT comparisons between corresponding elements and combining them with OR logic.

## Definition
```c
static Node *make_row_distinct_op(ParseState *pstate, List *opname, RowExpr *lrow, RowExpr *rrow, int location)
```

## Detailed Description
The `make_row_distinct_op` function handles row-level DISTINCT FROM operations by comparing each corresponding pair of elements from two row expressions. It iterates through the arguments of both row expressions, creating individual DISTINCT comparisons using `make_distinct_op` for each pair. The individual comparisons are then combined using OR logic - if any pair of corresponding elements is distinct, the entire row comparison is considered distinct. The function validates that both row expressions have the same number of elements before processing. For zero-length rows, it returns a constant FALSE value since empty rows are considered identical.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parsing context and error reporting
- `opname`: List containing the operator name (typically "IS DISTINCT FROM")
- `lrow`: RowExpr pointer representing the left side row expression
- `rrow`: RowExpr pointer representing the right side row expression  
- `location`: Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [make_distinct_op](make_distinct_op.md)
  - [makeBoolExpr](makeBoolExpr.md)
  - list_make2
  - [makeBoolConst](makeBoolConst.md)
  - list_length
  - forboth
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - transformAExprDistinct

## Notes and Other Information
- Combines individual DISTINCT comparisons using OR logic - any distinct pair makes the entire row distinct
- Validates equal length of row expressions before processing
- Handles zero-length rows by returning constant FALSE (empty rows are identical)
- Uses `make_distinct_op` for individual element comparisons to leverage existing DISTINCT semantics
- Builds the result incrementally by chaining OR expressions for each pairwise comparison
- The function implements the SQL standard semantics where row DISTINCT considers NULL values properly