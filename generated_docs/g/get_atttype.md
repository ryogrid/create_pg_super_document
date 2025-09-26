# get_atttype

## Location
[src/backend/utils/cache/lsyscache.c:913-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L913-L942)

## Overview
Retrieves the data type OID of an attribute from the PostgreSQL system catalog for a given relation and attribute number.

## Definition
```c
Oid get_atttype(Oid relid, AttrNumber attnum)
```

## Detailed Description
This function performs a system cache lookup to retrieve the atttypid field from the pg_attribute catalog table. The atttypid field contains the Object Identifier (OID) of the data type associated with the specified attribute. This OID can be used to look up detailed type information from the pg_type catalog. If the attribute doesn't exist or has been dropped, the function returns InvalidOid instead of throwing an error.

## Parameters / Member Variables
- `relid`: Object identifier of the relation (table/view/etc.) containing the attribute
- `attnum`: Attribute number (column number) within the relation

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) - performs the system cache lookup using ATTNUM cache
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) - converts relation OID to Datum format
  - [Int16GetDatum](../I/Int16GetDatum.md) - converts attribute number to Datum format
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md) - releases the system cache entry
  - InvalidOid - constant representing an invalid/non-existent OID

- Called from (representative examples):
  - [GetIndexInputType](../G/GetIndexInputType.md) (access/spgist/spgutils.c:129)
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md) (parser/parse_type.c:150)
  - [generateClonedIndexStmt](generateClonedIndexStmt.md) (parser/parse_utilcmd.c:1709)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md) (parser/parse_utilcmd.c:3453)

## Notes and Other Information
- Returns InvalidOid if the attribute doesn't exist or has been dropped, rather than throwing an error
- The returned OID can be used with other type-related functions to get detailed type information
- Essential for type checking and validation in query planning and execution
- Used in index creation, type lookups, and DDL operations that need to understand column types
- Part of the PostgreSQL type system infrastructure
- Commonly used in conjunction with type catalog functions to build complete type information
- The OID returned refers to an entry in the pg_type system catalog