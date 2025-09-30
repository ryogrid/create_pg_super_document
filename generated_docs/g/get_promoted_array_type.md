# get_promoted_array_type

## Location
[src/backend/utils/cache/lsyscache.c:2811-2831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2811-L2831)

## Overview
Determines the "promoted" array type for a given type OID, implementing the array type promotion logic used in ARRAY(SELECT ...) constructs by returning either the corresponding array type for scalar types or the same type if already an array.

## Definition
```c
Oid get_promoted_array_type(Oid typid)
```

## Detailed Description
This function implements PostgreSQL's array type promotion rules, which are essential for operations like `ARRAY(SELECT ...)` constructs. The promotion logic follows two rules:

1. If the input is a scalar type that has a corresponding "true" array type, return that array type
2. If the input is already a "true" array type, return the same type unchanged

The function first attempts to find an array type for the given type using `get_array_type()`. If that succeeds, it returns the array type. If not, it checks whether the input type is already an array type using `get_element_type()`. If the input is an array (has a valid element type), it returns the input type unchanged. If neither condition is met, it returns `InvalidOid`.

This promotion logic is crucial for maintaining type consistency in SQL operations that involve arrays and ensures that array construction operations behave predictably regardless of whether the input is already an array or a scalar type.

## Parameters / Member Variables
- `typid`: The OID of the type to be promoted to an array type

## Dependencies
- Functions called/Symbols referenced:
  - [get_array_type](get_array_type.md)
  - [get_element_type](get_element_type.md)
  - OidIsValid
- Called from (representative examples):
  - [exprType](../e/exprType.md)
  - [build_subplan](../b/build_subplan.md)

## Notes and Other Information
- Returns `InvalidOid` if the input type cannot be promoted to an array type
- The "promoted" type concept is specifically designed for `ARRAY(SELECT ...)` constructs
- The function handles both scalar-to-array promotion and array-to-array identity cases
- This is part of PostgreSQL's type promotion system, ensuring consistent behavior in array operations
- The two-step checking process (first try to get array type, then check if already array) implements the complete promotion logic

## Simplified Source

```c
Oid
get_promoted_array_type(Oid typid)
{
    // Try to get array type for scalar input
    Oid array_type = get_array_type(typid);
    if (OidIsValid(array_type))
        return array_type;

    // Check if input is already an array type
    if (OidIsValid(get_element_type(typid)))
        return typid;

    // Neither scalar with array type nor existing array
    return InvalidOid;
}
```