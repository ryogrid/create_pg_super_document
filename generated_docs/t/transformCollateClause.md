# transformCollateClause

## Location
[src/backend/parser/parse_expr.c:2776-2815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2776-L2815)

## Overview
Handles explicit COLLATE clauses in PostgreSQL by transforming the argument expression and looking up the specified collation name to create a CollateExpr node.

## Definition
```c
static Node *transformCollateClause(ParseState *pstate, CollateClause *c)
```

## Detailed Description
The `transformCollateClause` function processes explicit COLLATE clauses in SQL expressions (e.g., `expr COLLATE "en_US"`). It creates a new CollateExpr node that wraps the transformed argument expression with collation information. The function first transforms the argument expression recursively, then validates that the expression's data type supports collation. It performs a type check to ensure the argument type is collatable, with a special exception for the UNKNOWN type which is handled separately by coerce_type(). After validation, it looks up the specified collation name and stores the collation OID in the CollateExpr node along with location information for error reporting.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing the current parsing context and state information
- `c`: CollateClause pointer containing the argument expression and collation name to be applied

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [transformExprRecurse](transformExprRecurse.md)
  - [exprType](../e/exprType.md)
  - [type_is_collatable](type_is_collatable.md)
  - [LookupCollation](../L/LookupCollation.md)
  - [format_type_be](../f/format_type_be.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function validates that the argument type supports collation, rejecting non-collatable types except for UNKNOWNOID
- Creates a CollateExpr node that encapsulates both the expression and its collation information
- Preserves location information for accurate error reporting
- The UNKNOWN type receives special handling as it's processed separately by coerce_type()
- Reports clear error messages when attempting to apply collations to non-collatable data types

## Simplified Source

```c
static Node *
transformCollateClause(ParseState *pstate, CollateClause *c)
{
    CollateExpr *newc = makeNode(CollateExpr);

    // Transform the argument expression
    newc->arg = (Expr *) transformExprRecurse(pstate, c->arg);

    // Get the argument's data type
    Oid argtype = exprType((Node *) newc->arg);

    // Check if the type supports collation (unknown type is allowed)
    if (!type_is_collatable(argtype) && argtype != UNKNOWNOID)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("collations are not supported by type %s",
                        format_type_be(argtype)),
                 parser_errposition(pstate, c->location)));

    // Look up the collation name and store its OID
    newc->collOid = LookupCollation(pstate, c->collname, c->location);
    newc->location = c->location;

    return (Node *) newc;
}
```