# aclcheck_error_type

## Location
[src/backend/catalog/aclchk.c:3024-3035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3024-L3035)

## Overview
Specialized error reporting function for type-related access control failures, with special handling for array types to display the underlying element type in error messages.

## Definition
```c
void aclcheck_error_type(AclResult aclerr, Oid typeOid)
```

## Detailed Description
This function provides specialized error reporting for PostgreSQL type access control violations. Its key feature is intelligent handling of array types - when given an array type OID, it automatically resolves to the underlying element type for clearer error messages.

For example, instead of showing "permission denied for type _int4" (the internal array type name), it will show "permission denied for type integer" (the element type name). This makes error messages much more user-friendly since users typically think in terms of element types rather than the internal array type representations.

The function uses get_element_type() to detect if the given type is an array and extract its element type. If it's not an array type, get_element_type() returns InvalidOid and the original type is used. The type name is formatted using format_type_be() for consistent, user-readable output.

## Parameters / Member Variables
- `aclerr`: The result code from an ACL check (AclResult enum: ACLCHECK_OK, ACLCHECK_NO_PRIV, ACLCHECK_NOT_OWNER)
- `typeOid`: The OID of the PostgreSQL type for which access was checked

## Dependencies
- Functions called/Symbols referenced:
  - [get_element_type](../g/get_element_type.md) (to resolve array types to element types)
  - [aclcheck_error](aclcheck_error.md) (for actual error reporting)
  - [format_type_be](../f/format_type_be.md) (for formatted type name display)
  - OBJECT_TYPE constant
- Called from (representative examples):
  - [check_object_ownership](../c/check_object_ownership.md) (src/backend/catalog/objectaddress.c:2406)
  - [AggregateCreate](../A/AggregateCreate.md) (src/backend/catalog/pg_aggregate.c:591)
  - [compute_return_type](../c/compute_return_type.md) (src/backend/commands/functioncmds.c:160)
  - [DefineOperator](../D/DefineOperator.md) (src/backend/commands/operatorcmds.c:194)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:881)
  - [DefineDomain](../D/DefineDomain.md) (src/backend/commands/typecmds.c:790)

## Notes and Other Information
- This function never returns for error conditions - it delegates to aclcheck_error which throws an exception
- Primary benefit is user-friendly error messages for array types by showing element type names instead of internal array type names
- Uses format_type_be() for consistent type name formatting across PostgreSQL
- Automatically handles both simple types and array types with the same interface
- Part of the family of specialized aclcheck_error functions (aclcheck_error, aclcheck_error_col, aclcheck_error_type)
- Essential for operations involving type permissions like aggregate creation, operator definition, and domain creation

## Simplified Source

```c
void aclcheck_error_type(AclResult aclerr, Oid typeOid) {
    // Get element type for arrays (returns InvalidOid for non-arrays)
    Oid element_type = get_element_type(typeOid);

    // Use element type if available, otherwise use original type
    Oid display_type = element_type ? element_type : typeOid;

    // Report error with formatted type name
    aclcheck_error(aclerr, OBJECT_TYPE, format_type_be(display_type));
}
```