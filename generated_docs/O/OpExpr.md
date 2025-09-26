# OpExpr

## Location
src/include/nodes/primnodes.h: 813 - 840

## Overview
OpExpr represents an operator invocation in PostgreSQL's expression tree, semantically equivalent to a function call but using operator syntax.

## Definition
```c
typedef struct OpExpr
{
    Expr        xpr;

    /* PG_OPERATOR OID of the operator */
    Oid         opno;

    /* PG_PROC OID of underlying function */
    Oid         opfuncid pg_node_attr(equal_ignore_if_zero, query_jumble_ignore);

    /* PG_TYPE OID of result value */
    Oid         opresulttype pg_node_attr(query_jumble_ignore);

    /* true if operator returns set */
    bool        opretset pg_node_attr(query_jumble_ignore);

    /* OID of collation of result */
    Oid         opcollid pg_node_attr(query_jumble_ignore);

    /* OID of collation that operator should use */
    Oid         inputcollid pg_node_attr(query_jumble_ignore);

    /* arguments to the operator (1 or 2) */
    List       *args;

    /* token location, or -1 if unknown */
    ParseLoc    location;
} OpExpr;
```

## Detailed Description
OpExpr is a fundamental expression node type that represents operator invocations in PostgreSQL's query tree. It encapsulates both unary and binary operators (1 or 2 arguments) and maintains the relationship between operators and their underlying implementation functions.

The opfuncid field may not be filled immediately upon node creation - it's typically resolved by the planner before execution. During parsing and early planning phases, opfuncid can be 0, and the equal() function treats zero values as equal to accommodate this. Various internal state and collation fields are ignored during query jumbling to ensure equivalent queries generate the same plan cache keys.

The node supports PostgreSQL's operator resolution system, where operators are looked up in pg_operator and mapped to underlying implementation functions in pg_proc. This design allows for operator overloading and type-specific operator implementations.

## Parameters / Member Variables
- `xpr`: Base Expr node structure containing common expression fields
- `opno`: OID referencing the operator definition in pg_operator catalog
- `opfuncid`: OID of the underlying implementation function in pg_proc (may be 0 during parsing)
- `opresulttype`: OID of the data type returned by this operator expression
- `opretset`: Boolean flag indicating whether the operator returns a set of values
- `opcollid`: OID of the collation to be used for the result value
- `inputcollid`: OID of the collation that the operator implementation should use
- `args`: List of 1-2 argument expressions (operands for unary/binary operators)
- `location`: Parse location of the operator token in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - make_opclause (creates OpExpr nodes)
  - make_op (operator resolution and creation)
  - ExecInitExprRec (expression initialization)
  - clauselist_selectivity_ext (selectivity estimation)
  - match_clause_to_indexcol (index usage analysis)
  - eval_const_expressions_mutator (constant folding)
  - get_rule_expr (query deparsing)
  - CommuteOpExpr (operator commutation)

## Notes and Other Information
- Semantically equivalent to function calls but uses operator syntax (e.g., `a + b` vs `add(a, b)`)
- Supports both prefix (unary) and infix (binary) operator notation
- The equal() function has special handling for zero opfuncid values during planning phases
- Query jumbling ignores several fields (opfuncid, opresulttype, etc.) to ensure plan cache effectiveness
- Critical for index usage analysis, join condition evaluation, and WHERE clause processing
- Extensively used throughout the optimizer for selectivity estimation, predicate analysis, and plan optimization