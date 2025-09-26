# get_array_type

## Location
[src/backend/utils/cache/lsyscache.c:2787-2810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2787-L2810)

## Overview
Retrieves the OID of the corresponding "true" array type for a given base type OID, providing the fundamental mechanism for PostgreSQL's type system to locate array counterparts of scalar types.

## Definition

```c
Oid
get_array_type(Oid typid)
```
## Detailed Description
This function performs a system catalog lookup to find the array type that corresponds to a given base type. It searches the  system catalog using the provided type OID and extracts the  field, which contains the OID of the corresponding array type. The function is essential for PostgreSQL's type system, enabling operations that need to work with arrays of specific element types. If no array type exists for the given base type, the function returns .

The function uses the system cache (syscache) for efficient lookups, which provides faster access to frequently accessed catalog information compared to direct table scans.

## Parameters / Member Variables
- : The OID of the base type for which to find the corresponding array type

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - Form_pg_type
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [get_promoted_array_type](get_promoted_array_type.md)
  - [transformArrayExpr](../t/transformArrayExpr.md)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [make_scalar_array_op](../m/make_scalar_array_op.md)
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md)
  - [initArrayResultAny](../i/initArrayResultAny.md)

## Notes and Other Information
- Returns  if no array type can be found for the given base type
- Uses the system cache (TYPEOID cache) for efficient lookups
- The function accesses the  field of the  catalog entry
- This is a fundamental utility function used throughout the PostgreSQL codebase for type system operations
- The function is located in the system cache utilities module (), indicating its role as a low-level type system utility