# get_conversion_oid

## Location
[src/backend/catalog/namespace.c:4025-4079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4025-L4079)

## Overview
Finds a conversion by its possibly qualified name and returns its OID from the system catalog.

## Definition
```c
Oid get_conversion_oid(List *conname, bool missing_ok)
```

## Detailed Description
This function searches for a conversion by name, which can be either a simple name or a schema-qualified name. If a schema is explicitly provided, the function searches only in that schema using the system cache. Otherwise, it searches through the active search path, skipping the temporary namespace. The function uses GetSysCacheOid2 with the CONNAMENSP cache to efficiently lookup conversions by name and namespace. It can either raise an error or return InvalidOid when the conversion is not found, depending on the missing_ok parameter.

## Parameters / Member Variables
- `conname`: A List containing the conversion name, possibly schema-qualified
- `missing_ok`: If true, returns InvalidOid when conversion not found; if false, raises an error

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - GetSysCacheOid2
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - RangeVarGetRelid

## Notes and Other Information
- Uses the CONNAMENSP system cache for efficient lookups
- Skips the temporary namespace when searching through the search path
- Returns InvalidOid if conversion not found and missing_ok is true
- Raises ERRCODE_UNDEFINED_OBJECT error if conversion not found and missing_ok is false
- The function searches both explicitly qualified names and through the namespace search path