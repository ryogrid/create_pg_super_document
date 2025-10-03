# coerce_to_specific_type_typmod

## Location
[src/backend/parser/parse_coerce.c:1208-1256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1208-L1256)

## Overview
This function coerces an expression to a specific data type with a specific typmod, while ensuring the input is not a set.

## Definition

```c
Node *
coerce_to_specific_type_typmod(ParseState *pstate, Node *node,
							   Oid targetTypeId, int32 targetTypmod,
							   const char *constructName)
```
## Detailed Description
The coerce_to_specific_type_typmod function is a specialized coercion function that enforces both type and type modifier (typmod) requirements for SQL constructs that need precise data type specifications. It is similar to coerce_to_boolean but for any specific target type.

The function performs two main operations:
1. **Type and Typmod Coercion**: If the input expression type doesn't match the target type, it attempts to coerce the expression using assignment-level coercion rules. The coercion includes both the target type OID and the specific typmod value (such as precision/scale for numeric types, length for varchar, etc.).

2. **Set Validation**: Like coerce_to_boolean, it ensures that the expression does not return a set, which is typically not allowed in contexts requiring specific scalar types.

The function uses COERCION_ASSIGNMENT context, providing appropriate flexibility for implicit conversions while maintaining type safety.

## Parameters / Member Variables
- `*pstate`: Parse state for error reporting (can be NULL if special unknown-Param processing is not needed)
- `*node`: The input expression node to be coerced
- `targetTypeId`: OID of the required target data type
- `targetTypmod`: Specific type modifier required for the target type
- `*constructName`: Name of the SQL construct requiring this specific type (for error messages)
## Dependencies
- Functions called/Symbols referenced:
  - [exprType](../e/exprType.md) (get expression type)
  - [coerce_to_target_type](coerce_to_target_type.md) (perform type coercion with typmod)
  - [expression_returns_set](../e/expression_returns_set.md) (check for set-returning expressions)
  - [exprLocation](../e/exprLocation.md) (get source location for errors)
  - [format_type_be](../f/format_type_be.md), parser_errposition (error reporting)
  - COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST (constants)
- Called from:
  - [transformRangeTableFunc](../t/transformRangeTableFunc.md) (table function processing)
  - [coerce_to_specific_type](coerce_to_specific_type.md) (wrapper function)

## Notes and Other Information
- This is a public function exposed through parse_coerce.h
- More specific than coerce_to_boolean as it handles any target type with typmod requirements
- Uses COERCION_ASSIGNMENT context for balanced flexibility and type safety
- The typmod parameter allows for precise type specifications (e.g., VARCHAR(50), NUMERIC(10,2))
- Commonly used in contexts where exact type specifications are required
- Provides clear error messages indicating both the construct name and expected vs. actual types
- Set-returning functions are prohibited, maintaining scalar value requirements

## Simplified Source

```c
Node *
coerce_to_specific_type_typmod(ParseState *pstate, Node *node,
                               Oid targetTypeId, int32 targetTypmod,
                               const char *constructName)
{
    Oid inputTypeId = exprType(node);

    // Coerce type if needed
    if (inputTypeId != targetTypeId) {
        Node *newnode = coerce_to_target_type(pstate, node, inputTypeId,
                                              targetTypeId, targetTypmod,
                                              COERCION_ASSIGNMENT,
                                              COERCE_IMPLICIT_CAST, -1);
        if (newnode == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("argument of %s must be type %s, not type %s",
                            constructName,
                            format_type_be(targetTypeId),
                            format_type_be(inputTypeId)),
                     parser_errposition(pstate, exprLocation(node))));
        node = newnode;
    }

    // Ensure expression doesn't return a set
    if (expression_returns_set(node))
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("argument of %s must not return a set", constructName),
                 parser_errposition(pstate, exprLocation(node))));

    return node;
}
```