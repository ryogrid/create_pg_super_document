# make_distinct_op

## Location
[src/backend/parser/parse_expr.c:3062-3096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3062-L3096)

## Overview
Creates a DistinctExpr node for IS DISTINCT FROM operators by leveraging the equality operator infrastructure and converting the result to proper DISTINCT semantics.

## Definition
```c
static Expr *make_distinct_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree, int location)
```

## Detailed Description
The `make_distinct_op` function constructs a DistinctExpr node to handle IS DISTINCT FROM operations. It first uses `make_op` to create a regular operator expression (typically an equality operator), then validates that the operator returns boolean and doesn't return a set. The key insight is that DistinctExpr and OpExpr have the same internal structure, so the function simply changes the node tag from T_OpExpr to T_DistinctExpr using `NodeSetTag`. This transformation allows the expression to be processed with IS DISTINCT FROM semantics during execution, which differs from regular equality in its handling of NULL values - NULL IS DISTINCT FROM NULL returns false, while NULL = NULL returns NULL.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parsing context and error reporting
- `opname`: List containing the operator name (typically equality operator for DISTINCT)
- `ltree`: Node pointer representing the left operand expression
- `rtree`: Node pointer representing the right operand expression
- `location`: Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [make_op](make_op.md)
  - NodeSetTag
  - [parser_errposition](../p/parser_errposition.md)
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - transformAExprDistinct
  - [make_row_distinct_op](make_row_distinct_op.md)

## Notes and Other Information
- Relies on the structural equivalence between DistinctExpr and OpExpr nodes
- Validates that the underlying operator returns boolean type and doesn't return sets
- The node tag conversion from T_OpExpr to T_DistinctExpr changes execution semantics for NULL handling
- IS DISTINCT FROM treats NULL values specially: NULL IS DISTINCT FROM NULL is false, unlike NULL = NULL which is NULL
- The function reuses existing equality operator resolution through make_op for consistency
- Error messages are marked for translation and specifically mention "IS DISTINCT FROM" construct