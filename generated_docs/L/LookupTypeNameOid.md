# LookupTypeNameOid

## Location
src/backend/parser/parse_type.c: 232 - 263

## Overview
LookupTypeNameOid provides a simplified interface to resolve a TypeName to its PostgreSQL type OID, returning InvalidOid if the type cannot be found.

## Definition
```c
Oid LookupTypeNameOid(ParseState *pstate, const TypeName *typeName, bool missing_ok)
```

## Detailed Description
LookupTypeNameOid serves as a convenience wrapper around LookupTypeName when only the type OID is needed rather than the full Type tuple. It performs type name resolution and extracts the OID from the resulting system cache entry, properly releasing the cache entry before returning. The function is commonly used in scenarios where type existence checking or OID retrieval is the primary goal, such as in DROP commands and function/operator lookups.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location reporting (may be NULL)
- `typeName`: TypeName structure containing the type specification to resolve
- `missing_ok`: Boolean controlling error behavior when type is not found (true = return InvalidOid, false = raise error)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](LookupTypeName.md)
  - [TypeNameToString](../T/TypeNameToString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [get_object_address](../g/get_object_address.md)
  - [type_in_list_does_not_exist_skipping](../t/type_in_list_does_not_exist_skipping.md)
  - [LookupFuncWithArgs](LookupFuncWithArgs.md)
  - [LookupOperWithArgs](LookupOperWithArgs.md)

## Notes and Other Information
Located in src/backend/parser/parse_type.c:232-263. Important: the returned OID may correspond to a shell type, so callers need to be aware of this limitation. Most code should use typenameTypeId instead, which provides additional validation. The function properly manages system cache resources by releasing the Type tuple after extracting the OID.