# verify_common_type_from_oids

## Location
[src/backend/parser/parse_coerce.c:1628-1645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L1628-L1645)

## Overview
Verifies that all input type OIDs can be implicitly coerced to a proposed common type, serving as a static variant of verify_common_type().

## Definition
```c
static bool verify_common_type_from_oids(Oid common_type, int nargs, const Oid *typeids)
```

## Detailed Description
This static function performs the same validation as verify_common_type() but operates directly on an array of type OIDs rather than expression nodes. It iterates through each type OID in the array and uses can_coerce_type() to verify that implicit coercion to the common type is possible. The function returns true only if all types can be coerced; if any type fails the coercion check, it immediately returns false. This is useful when type checking needs to be performed without having access to the actual expression nodes, working purely at the type system level.

## Parameters / Member Variables
- `common_type`: OID of the proposed common type to verify against
- `nargs`: Number of type OIDs in the typeids array
- `typeids`: Array of type OIDs to check for coercion compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [can_coerce_type](../c/can_coerce_type.md) (to check coercion feasibility)
  - COERCION_IMPLICIT (coercion method constant)

- Called from (representative examples):
  - [check_generic_type_consistency](../c/check_generic_type_consistency.md) (generic type consistency checking)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md) (generic type consistency enforcement)

## Notes and Other Information
- Static function - only visible within parse_coerce.c
- Companion to verify_common_type() but works at the type OID level rather than expression level
- More efficient when only type information is available without full expression nodes
- Returns false immediately upon finding the first non-coercible type (short-circuit evaluation)
- Used internally by PostgreSQL's generic type consistency checking system
- Only checks for implicit coercion capability - does not perform actual coercion
- Located in src/backend/parser/parse_coerce.c:1628-1645

## Simplified Source

```c
static bool
verify_common_type_from_oids(Oid common_type, int nargs, const Oid *typeids)
{
    // Check if all input types can be coerced to common_type
    for (int i = 0; i < nargs; i++) {
        if (!can_coerce_type(1, &typeids[i], &common_type, COERCION_IMPLICIT))
            return false;  // Early exit if any type can't be coerced
    }
    return true;  // All types are coercible
}
```