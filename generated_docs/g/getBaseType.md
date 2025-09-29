# getBaseType

## Location
[src/backend/utils/cache/lsyscache.c:2521-2537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2521-L2537)

## Overview
Returns the base type OID for a given type, resolving domain types to their underlying base types while returning the same OID for non-domain types.

## Definition

```c
Oid
getBaseType(Oid typid)
```
## Detailed Description
This function provides a convenient interface for resolving PostgreSQL domain types to their underlying base types. A domain in PostgreSQL is a user-defined data type that is based on an existing type but can have additional constraints. When given a domain type OID, this function returns the OID of the underlying base type. For regular (non-domain) types, it simply returns the same type OID. Internally, it delegates to  with a dummy typmod parameter, making it a simplified wrapper for cases where the type modifier is not needed.

## Parameters / Member Variables
- : The OID of the type to resolve, which may be either a domain type or a regular type

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseTypeAndTypmod](getBaseTypeAndTypmod.md)
- Called from (representative examples):
  - [GetIndexInputType](../G/GetIndexInputType.md)
  - [find_expr_references_walker](../f/find_expr_references_walker.md)
  - [CheckAttributeType](../C/CheckAttributeType.md)
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md)
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)
  - [coerce_type](../c/coerce_type.md)
  - [select_common_type](../s/select_common_type.md)
  - [check_generic_type_consistency](../c/check_generic_type_consistency.md)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md)
  - [func_select_candidate](../f/func_select_candidate.md)
  - [binary_oper_exact](../b/binary_oper_exact.md)
  - [range_typanalyze](../r/range_typanalyze.md)
  - [type_is_rowtype](../t/type_is_rowtype.md)

## Notes and Other Information
- This function is essential for type system operations that need to work with the underlying type rather than domain wrappers
- Commonly used in type coercion, operator resolution, and constraint checking where domain constraints should be ignored
- Part of the lsyscache.c module which provides cached access to system catalog information
- The function is heavily used throughout the parser, type system, and executor for resolving type compatibility
- Domain types inherit most properties from their base types, so this function enables treating domains transparently in many contexts
- Returns the input type OID unchanged for built-in types, user-defined base types, and composite types
- Critical for generic type resolution in polymorphic functions and operators

## Simplified Source

```c
Oid
getBaseType(Oid typid)
{
    int32 typmod = -1;

    // Delegate to full function with dummy typmod parameter
    return getBaseTypeAndTypmod(typid, &typmod);
}
```