# coerce_to_common_type

## Location
[src/backend/parser/parse_coerce.c:1574-1607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1574-L1607)

## Overview
Coerces an expression node to a specified target type, used after select_common_type() to convert individual expressions to the desired common type.

## Definition

```c
Node *coerce_to_common_type(ParseState *pstate, Node *node, Oid targetTypeId, const char *context)
```

## Detailed Description
This function performs type coercion on an expression node to convert it to a target type that was previously determined by select_common_type(). It first checks if the input type matches the target type, returning the node unchanged if they're the same. If coercion is needed, it uses can_coerce_type() to verify that implicit coercion is possible, then calls coerce_type() to perform the actual conversion. If coercion is not possible, it reports an error with a descriptive message including the context.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error reporting and unknown parameter processing (may be NULL)
- `node`: The expression node to be coerced
- `targetTypeId`: OID of the target type to coerce to
- `context`: Descriptive string used in error messages (e.g., "CASE", "UNION")

## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md) (to get input type)
  - [can_coerce_type](can_coerce_type.md) (to check coercion feasibility)
  - [coerce_type](coerce_type.md) (to perform actual coercion)
  - ereport/errcode/errmsg (for error reporting)
  - [format_type_be](../f/format_type_be.md) (for type name formatting)
  - [parser_errposition](../p/parser_errposition.md) (for error location)
  - [exprLocation](../e/exprLocation.md) (to get node location)
  - COERCION_IMPLICIT (coercion method constant)
  - COERCE_IMPLICIT_CAST (coercion format constant)

- Called from (representative examples):
  - [transformCaseExpr](../t/transformCaseExpr.md) (CASE expression processing)
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (UNION/INTERSECT/EXCEPT operations)
  - [transformArrayExpr](../t/transformArrayExpr.md) (array construction)
  - [transformCoalesceExpr](../t/transformCoalesceExpr.md) (COALESCE function)
  - [generate_setop_tlist](../g/generate_setop_tlist.md) (set operation target list generation)

## Notes and Other Information
- Returns the original node unchanged if no coercion is needed (input type matches target type)
- Only attempts implicit coercion - explicit casts are handled elsewhere
- The context parameter provides user-friendly error messages specific to the SQL construct being processed
- Part of PostgreSQL's type coercion system that ensures type consistency in expressions
- Located in src/backend/parser/parse_coerce.c:1574-1607

## Simplified Source

```c
Node *
coerce_to_common_type(ParseState *pstate, Node *node,
                      Oid targetTypeId, const char *context)
{
    Oid inputTypeId = exprType(node);

    // No coercion needed if types match
    if (inputTypeId == targetTypeId)
        return node;

    // Check if implicit coercion is possible
    if (can_coerce_type(1, &inputTypeId, &targetTypeId, COERCION_IMPLICIT))
    {
        // Perform the coercion
        node = coerce_type(pstate, node, inputTypeId, targetTypeId, -1,
                          COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    }
    else
    {
        // Report coercion failure
        ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                 errmsg("%s could not convert type %s to %s",
                       context,
                       format_type_be(inputTypeId),
                       format_type_be(targetTypeId)),
                 parser_errposition(pstate, exprLocation(node))));
    }

    return node;
}
```