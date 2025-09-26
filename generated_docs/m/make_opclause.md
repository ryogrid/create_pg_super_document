# make_opclause

## Location
[src/backend/nodes/makefuncs.c:675-700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L675-L700)

## Overview
Creates an OpExpr node representing operator expressions in PostgreSQL's expression tree, supporting both binary and unary operators with collation information.

## Definition
```c
Expr *make_opclause(Oid opno, Oid opresulttype, bool opretset, Expr *leftop, Expr *rightop, Oid opcollid, Oid inputcollid)
```

## Detailed Description
The make_opclause function constructs an OpExpr node, which represents operator expressions in PostgreSQL's internal expression representation. This function handles both binary operators (with left and right operands) and unary operators (with only a left operand). The function initializes all the necessary fields for operator execution, including operator identification, result type, collation information, and operand expressions. The opfuncid field is initially set to InvalidOid and will be filled in later during planning when the actual function implementing the operator is resolved.

## Parameters / Member Variables
- `opno`: OID of the operator in the pg_operator system catalog
- `opresulttype`: OID of the data type returned by this operator
- `opretset`: Boolean indicating whether the operator returns a set of values (true) or a single value (false)
- `leftop`: Expression node for the left operand (required for both unary and binary operators)
- `rightop`: Expression node for the right operand (NULL for unary operators, non-NULL for binary operators)
- `opcollid`: OID of the collation to be used by the operator for comparison/ordering operations
- `inputcollid`: OID of the collation of the input expressions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate OpExpr node)
  - OpExpr (the node structure being created)
  - list_make1 (for unary operators)
  - list_make2 (for binary operators)
  - InvalidOid (constant for uninitialized OID fields)
- Called from (representative examples):
  - match_boolean_index_clause
  - expand_indexqual_rowcompare
  - process_implied_equality
  - build_implied_join_equality
  - convert_EXISTS_to_ANY
  - make_partition_op_expr
  - match_pattern_prefix

## Notes and Other Information
- The function automatically determines whether to create a unary or binary operator expression based on whether rightop is NULL
- The opfuncid field is set to InvalidOid initially and resolved later during query planning
- The location field is set to -1 (unknown location) since this function is typically called during internal transformations
- This function is heavily used in query optimization, partitioning, pattern matching, and other internal PostgreSQL operations
- Collation information is crucial for operators that perform text comparison or ordering operations
- The returned expression can be used in WHERE clauses, JOIN conditions, and other contexts requiring operator evaluation