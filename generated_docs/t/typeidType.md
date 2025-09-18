# typeidType

## Location
src/backend/parser/parse_type.c: 578 - 589

## Overview
typeidType is a utility function that retrieves a Type structure (system catalog tuple) for a given type OID from the PostgreSQL system cache.

## Definition
```c
Type typeidType(Oid id)
```

## Detailed Description
This function performs a system cache lookup to retrieve the pg_type catalog tuple for a specified type OID. It returns the tuple wrapped as a Type structure, which provides access to all metadata about the PostgreSQL data type including its name, size, alignment, input/output functions, and other properties.

The function uses the system cache for efficient lookup and will raise an ERROR if the type OID is not found, indicating either an invalid type ID or a corrupted system catalog.

## Parameters / Member Variables
- `id`: The OID of the type to look up in the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Type (return type)
  - HeapTuple
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md)
  - [find_typmod_coercion_function](../f/find_typmod_coercion_function.md)

## Notes and Other Information
- **Important**: The caller MUST call ReleaseSysCache() on the returned tuple when finished to avoid memory leaks
- The function will terminate with ERROR if the type OID is invalid, so callers should ensure the OID is valid
- Uses the TYPEOID cache for fast lookups of frequently accessed type information
- Returns a HeapTuple cast to Type, providing access to the complete pg_type catalog entry
- Located in src/backend/parser/parse_type.c:578-589
- This is a low-level function primarily used by type coercion and validation routines