# MinMaxExpr

## Location
[src/include/nodes/primnodes.h:1506-1521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1506-L1521)

## Overview
MinMaxExpr represents a GREATEST or LEAST function expression in PostgreSQL's expression tree, which returns either the maximum or minimum value from a list of expressions.

## Definition
```c
typedef enum MinMaxOp
{
    IS_GREATEST,
    IS_LEAST
} MinMaxOp;

typedef struct MinMaxExpr
{
    Expr        xpr;
    /* common type of arguments and result */
    Oid         minmaxtype pg_node_attr(query_jumble_ignore);
    /* OID of collation of result */
    Oid         minmaxcollid pg_node_attr(query_jumble_ignore);
    /* OID of collation that function should use */
    Oid         inputcollid pg_node_attr(query_jumble_ignore);
    /* function to execute */
    MinMaxOp    op;
    /* the arguments */
    List       *args;
    /* token location, or -1 if unknown */
    ParseLoc    location;
} MinMaxExpr;
```

## Detailed Description
MinMaxExpr is a node structure that represents the SQL GREATEST and LEAST functions in PostgreSQL's internal expression representation. The GREATEST function returns the largest value among its arguments, while LEAST returns the smallest. The structure stores all necessary information including the operation type (greatest vs least), result type, collation information, and the list of arguments to be evaluated.

The structure includes query jumble ignore attributes on type and collation fields, indicating these fields should be ignored when generating query fingerprints for query plan caching and statistics. The distinction between minmaxcollid (result collation) and inputcollid (function collation) allows for proper handling of collation-sensitive comparisons.

## Parameters / Member Variables
- `xpr`: Base expression node containing common expression information
- `minmaxtype`: OID of the common data type of the arguments and result
- `minmaxcollid`: OID of the collation of the result value
- `inputcollid`: OID of the collation that the comparison function should use
- `op`: MinMaxOp enum value specifying whether this is GREATEST (IS_GREATEST) or LEAST (IS_LEAST)
- `args`: List of expression nodes representing the arguments to compare
- `location`: Parse location in the original SQL text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - MinMaxOp (enum)
  - ParseLoc
  - Expr (base type)
  - List
  - Oid

- Called from (representative examples):
  - transformMinMaxExpr (parse_expr.c:2263, 2265)
  - ExecInitExprRec (execExpr.c:2186)
  - exprType (nodeFuncs.c:213)
  - cost_qual_eval_walker (costsize.c:4880)
  - get_rule_expr (ruleutils.c:9690)

## Notes and Other Information
- MinMaxExpr is part of PostgreSQL's expression node hierarchy, inheriting from the base Expr type
- The pg_node_attr(query_jumble_ignore) annotations help optimize query plan caching by excluding type and collation information from query fingerprinting
- GREATEST and LEAST functions are SQL standard functions commonly used for value comparison and selection
- The separation of result collation (minmaxcollid) and input collation (inputcollid) enables proper handling of collation rules during comparison operations
- The transformation from SQL GREATEST/LEAST syntax to this internal representation happens in transformMinMaxExpr function
- During execution, all arguments are evaluated and compared using the appropriate comparison operators for the data type