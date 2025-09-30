# transformAExprNullIf

## Location
[src/backend/parser/parse_expr.c:1083-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1083-L1125)

## Overview
Transforms A_Expr nodes representing NULLIF operations into NullIfExpr nodes, performing type validation and ensuring the comparison operator returns boolean results.

## Definition
```c
static Node *transformAExprNullIf(ParseState *pstate, A_Expr *a)
```

## Detailed Description
This function handles the transformation of SQL NULLIF expressions during expression parsing. NULLIF(expr1, expr2) returns NULL if expr1 equals expr2, otherwise it returns expr1.

The transformation process involves several steps:

1. **Expression Transformation**: Both left and right expressions are recursively transformed using transformExprRecurse.

2. **Operator Creation**: Creates an OpExpr using the equality operator specified in the A_Expr (typically '=').

3. **Type Validation**: Performs strict validation to ensure:
   - The comparison operator yields a boolean result (BOOLOID)
   - The operator does not return a set (opretset must be false)

4. **Result Type Adjustment**: The final NullIfExpr inherits the type of the first operand (not boolean), since NULLIF returns either NULL or the first argument's value.

5. **Node Conversion**: Converts the OpExpr structure to a NullIfExpr by changing the node tag, leveraging the fact that both structures are identical in memory layout.

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state and environment information
- `a`: A_Expr node representing the NULLIF expression to transform

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md)
  - [make_op](../m/make_op.md)
  - [exprType](../e/exprType.md)
  - linitial
  - NodeSetTag
  - ereport (for error handling)
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within parse_expr.c
- NULLIF expressions require strict type checking since they must use equality comparison
- The clever reuse of OpExpr structure for NullIfExpr saves memory and simplifies code
- Error messages are translatable and provide specific location information for debugging
- The result type adjustment is crucial because NULLIF semantically returns the first argument's type, not boolean
- Located in src/backend/parser/parse_expr.c:1083-1125

## Simplified Source

```c
static Node *
transformAExprNullIf(ParseState *pstate, A_Expr *a)
{
    // Transform both operands recursively
    Node *lexpr = transformExprRecurse(pstate, a->lexpr);
    Node *rexpr = transformExprRecurse(pstate, a->rexpr);

    // Create equality comparison operator
    OpExpr *result = (OpExpr *) make_op(pstate, a->name, lexpr, rexpr,
                                        pstate->p_last_srf, a->location);

    // Validate operator requirements for NULLIF
    if (result->opresulttype != BOOLOID)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("NULLIF requires = operator to yield boolean"),
                        parser_errposition(pstate, a->location)));

    if (result->opretset)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("NULLIF must not return a set"),
                        parser_errposition(pstate, a->location)));

    // Set result type to first operand's type (not boolean)
    result->opresulttype = exprType((Node *) linitial(result->args));

    // Convert OpExpr to NullIfExpr (same structure, different tag)
    NodeSetTag(result, T_NullIfExpr);

    return (Node *) result;
}
```