# make_nulltest_from_distinct

## Location
[src/backend/parser/parse_expr.c:3097-3120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3097-L3120)

## Overview
Creates a NullTest node from an IS [NOT] DISTINCT FROM NULL construct during SQL expression parsing.

## Definition
```c
static Node *make_nulltest_from_distinct(ParseState *pstate, A_Expr *distincta, Node *arg)
```

## Detailed Description
This function converts an IS [NOT] DISTINCT FROM NULL expression into a NullTest node. It serves as a helper function during expression transformation, specifically handling the case where a DISTINCT comparison involves a NULL value. The function creates the appropriate NullTest node with the correct null test type based on whether the original expression was DISTINCT or NOT DISTINCT.

The function determines the null test type by examining the kind of the A_Expr: if it's AEXPR_NOT_DISTINCT, it creates an IS_NULL test; otherwise, it creates an IS_NOT_NULL test. The argisrow field is always set to false, which is correct regardless of whether the argument is a composite type.

## Parameters / Member Variables
- `pstate`: ParseState context for the current parsing operation
- `distincta`: The A_Expr node representing the DISTINCT comparison expression
- `arg`: The untransformed argument node that will be tested for nullness

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create NullTest node)
  - [transformExprRecurse](../t/transformExprRecurse.md) (to transform the argument expression)
  - [A_Expr](../A/A_Expr.md) (input parameter type)
  - [NullTest](../N/NullTest.md) (created node type)
  - AEXPR_NOT_DISTINCT (expression kind constant)
  - IS_NULL (null test type constant)
  - IS_NOT_NULL (null test type constant)
- Called from (representative examples):
  - [transformAExprDistinct](../t/transformAExprDistinct.md)

## Notes and Other Information
- This is a static helper function within parse_expr.c
- The function handles the semantic conversion from DISTINCT FROM NULL syntax to internal NullTest representation
- The argisrow field is always set to false, which correctly handles both scalar and composite argument types
- Location information is preserved from the original expression for error reporting

## Simplified Source

```c
static Node *
make_nulltest_from_distinct(ParseState *pstate, A_Expr *distincta, Node *arg)
{
    NullTest *nt = makeNode(NullTest);

    // Transform the argument expression
    nt->arg = (Expr *) transformExprRecurse(pstate, arg);

    // Set null test type based on DISTINCT expression kind
    if (distincta->kind == AEXPR_NOT_DISTINCT)
        nt->nulltesttype = IS_NULL;      // NOT DISTINCT FROM NULL -> IS NULL
    else
        nt->nulltesttype = IS_NOT_NULL;  // DISTINCT FROM NULL -> IS NOT NULL

    // Set standard fields
    nt->argisrow = false;  // Correct for both scalar and composite types
    nt->location = distincta->location;

    return (Node *) nt;
}
```