# verify_common_type

## Location
[src/backend/parser/parse_coerce.c:1608-1627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1608-L1627)

## Overview
Verifies that all input expressions can be implicitly coerced to a proposed common type, returning true if all coercions are possible.

## Definition
```c
bool verify_common_type(Oid common_type, List *exprs)
```

## Detailed Description
This function performs a validation check to ensure that all expressions in a list can be implicitly coerced to a specified common type. It iterates through each expression, extracts its type using exprType(), and checks if implicit coercion to the common type is possible using can_coerce_type(). The function returns true only if all expressions can be coerced; if any expression fails the coercion check, it immediately returns false. This is typically used as a separate validation step when the caller needs to verify the applicability of a common type before proceeding with actual coercion operations.

## Parameters / Member Variables
- `common_type`: OID of the proposed common type to verify against
- `exprs`: List of expression nodes to check for coercion compatibility

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (list iteration macro)
  - [exprType](../e/exprType.md) (to extract expression type)
  - [can_coerce_type](../c/can_coerce_type.md) (to check coercion feasibility)
  - COERCION_IMPLICIT (coercion method constant)

- Called from (representative examples):
  - [transformAExprIn](../t/transformAExprIn.md) (IN expression processing)

## Notes and Other Information
- Most callers of select_common_type() don't need this explicit verification since coercion checks happen automatically during expression conversion
- Useful when a separate validation step is needed before attempting actual coercion
- Only checks for implicit coercion capability - does not perform the actual coercion
- Returns false immediately upon finding the first non-coercible expression (short-circuit evaluation)
- Part of PostgreSQL's type coercion validation system
- Located in src/backend/parser/parse_coerce.c:1608-1627