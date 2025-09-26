# CoalesceExpr

## Location
src/include/nodes/primnodes.h: 1484 - 1495

## Overview
CoalesceExpr represents a COALESCE expression in PostgreSQL's expression tree, which returns the first non-null value from a list of expressions.

## Definition


## Detailed Description
CoalesceExpr is a node structure that represents the SQL COALESCE function in PostgreSQL's internal expression representation. The COALESCE function evaluates its arguments from left to right and returns the first non-null value. If all arguments are null, it returns null. This structure stores all necessary information including the result type, collation information, and the list of arguments to be evaluated.

The structure includes query jumble ignore attributes on type and collation fields, indicating these fields should be ignored when generating query fingerprints for query plan caching and statistics.

## Parameters / Member Variables
- : Base expression node containing common expression information
- : OID of the data type that the COALESCE expression will return
- : OID of the collation to use for the result, or InvalidOid if no collation applies
- : List of expression nodes representing the arguments to COALESCE
- : Parse location in the original SQL text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
  - Expr (base type)
  - List
  - Oid

- Called from (representative examples):
  - transformCoalesceExpr (parse_expr.c:2214, 2216)
  - ExecInitExprRec (execExpr.c:2138)
  - exprType (nodeFuncs.c:210)
  - eval_const_expressions_mutator (clauses.c:3292, 3293, 3334)
  - get_rule_expr (ruleutils.c:9680)

## Notes and Other Information
- CoalesceExpr is part of PostgreSQL's expression node hierarchy, inheriting from the base Expr type
- The pg_node_attr(query_jumble_ignore) annotations help optimize query plan caching by excluding type and collation information from query fingerprinting
- COALESCE expressions are commonly used in SQL for handling null values and providing default values
- The transformation from SQL COALESCE syntax to this internal representation happens in transformCoalesceExpr function
- During execution, the expression evaluator processes the arguments sequentially until finding a non-null value