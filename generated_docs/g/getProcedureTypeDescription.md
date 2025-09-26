# getProcedureTypeDescription

## Location
[src/backend/catalog/objectaddress.c:4703-4739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4703-L4739)

## Overview
A static helper function that appends a human-readable description of a procedure type ("aggregate", "procedure", or "function") to a StringInfo buffer based on the procedure's kind stored in the pg_proc catalog.

## Definition

```c
static void
getProcedureTypeDescription(StringInfo buffer, Oid procid,
							bool missing_ok)
```
## Detailed Description
This function serves as a subroutine for getObjectTypeDescription to provide accurate type descriptions for procedure-like objects in PostgreSQL. It looks up the procedure in the pg_proc system catalog using the provided OID and examines the prokind field to determine whether the object is an aggregate function, a stored procedure, or a regular function (including window functions). The appropriate type string is then appended to the provided StringInfo buffer.

The function handles missing procedures gracefully when missing_ok is true, falling back to the generic term "routine" if the procedure cannot be found in the catalog.

## Parameters / Member Variables
- : StringInfo buffer where the procedure type description will be appended
- : Object ID (OID) of the procedure to describe
- : Boolean flag indicating whether to handle missing procedures gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging)
  - [appendStringInfoString](../a/appendStringInfoString.md) (string buffer operations)
  - GETSTRUCT (tuple data extraction)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID conversion)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_proc (pg_proc catalog structure)
  - PROKIND_AGGREGATE (procedure kind constant)
  - PROKIND_PROCEDURE (procedure kind constant)

- Called from (representative examples):
  - [getObjectTypeDescription](getObjectTypeDescription.md) (primary caller for object type descriptions)
  - object_type_map (object type mapping structure)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the objectaddress.c compilation unit
- The function uses PostgreSQL's system cache (SearchSysCache1) for efficient catalog lookups
- When missing_ok is true and the procedure is not found, it uses "routine" as a generic fallback term
- The function distinguishes between three types of callable objects: aggregates, procedures, and functions (including window functions)
- Proper cache management is implemented with ReleaseSysCache to prevent memory leaks
- The function is part of PostgreSQL's object address and identification infrastructure