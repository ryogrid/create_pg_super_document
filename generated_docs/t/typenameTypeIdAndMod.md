# typenameTypeIdAndMod

## Location
[src/backend/parser/parse_type.c:310-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L310-L331)

## Overview
A utility function that extracts the type OID and type modifier from a TypeName structure, providing a lightweight alternative to typenameType that returns only the essential identifiers without the full syscache entry.

## Definition

```c
void
typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p)
```
## Detailed Description
This function serves as a wrapper around  that simplifies access to just the type OID and type modifier information. It internally calls  to perform the full type lookup and validation, then extracts only the OID and typmod values from the returned syscache entry before releasing it. This approach is more efficient when the caller only needs the basic type identification information rather than the complete type tuple.

The function handles the syscache management automatically, ensuring proper cleanup of the temporary type tuple after extracting the required information.

## Parameters / Member Variables
- `*pstate`: Parse state context for error reporting and namespace resolution
- `*typeName`: Input TypeName structure containing the type specification to resolve
- `*typeid_p`: Output parameter to receive the resolved type's OID
- `*typmod_p`: Output parameter to receive the type modifier value
## Dependencies
- Functions called/Symbols referenced:
  - [typenameType](typenameType.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [MergeChildAttribute](../M/MergeChildAttribute.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md)
  - [transformTypeCast](transformTypeCast.md)
  - [transformRangeTableFunc](transformRangeTableFunc.md)

## Notes and Other Information
This function is preferred over  when the caller only needs the type OID and typmod values, as it handles the syscache cleanup automatically and provides a cleaner interface. It's commonly used in DDL operations, type casting, and other scenarios where type identification is needed without requiring access to the full type catalog information.

## Simplified Source
```c
void typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
                         Oid *typeid_p, int32 *typmod_p)
{
    // Get the full type tuple from system catalog
    Type tup = typenameType(pstate, typeName, typmod_p);

    // Extract just the type OID from the tuple
    *typeid_p = ((Form_pg_type) GETSTRUCT(tup))->oid;

    // Clean up the syscache entry
    ReleaseSysCache(tup);
}
```