# get_typlen

## Location
[src/backend/utils/cache/lsyscache.c:2197-2221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2197-L2221)

## Overview
Retrieves the storage length of a PostgreSQL data type given its OID, providing essential size information for memory allocation and data handling operations.

## Definition

```c
int16
get_typlen(Oid typid)
```
## Detailed Description
The  function is a utility function that looks up the storage length () of a PostgreSQL data type in the system catalog. It performs a system cache lookup on the  catalog to retrieve the type's length information. The function returns the  field from the type's catalog entry, which indicates how many bytes the type occupies in storage. For variable-length types, this returns -1, while fixed-length types return their actual byte size.

This function is part of PostgreSQL's type system infrastructure and is frequently used throughout the codebase when type size information is needed for memory allocation, tuple construction, and data serialization/deserialization operations.

## Parameters / Member Variables
- : The OID (Object Identifier) of the PostgreSQL data type whose length is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_type (type catalog structure)
- Called from (representative examples):
  - [DefineType](../D/DefineType.md) (type creation command)
  - [ExecBuildProjectionInfo](../E/ExecBuildProjectionInfo.md) (executor projection setup)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [array_exec_setup](../a/array_exec_setup.md) (array subscription setup)
  - [pg_column_size](../p/pg_column_size.md) (column size calculation)
  - [get_typavgwidth](get_typavgwidth.md) (average width calculation)

## Notes and Other Information
- Returns 0 if the type OID is invalid or not found in the catalog
- For variable-length types (like text, varchar), returns -1
- For fixed-length types, returns the actual byte size (e.g., 4 for int4, 8 for int8)
- The function uses the system cache for performance, avoiding direct catalog table access
- This is a fundamental building block for PostgreSQL's type system and is used extensively in query execution, type coercion, and storage management

## Simplified Source

```c
int16 get_typlen(Oid typid) {
    // Look up the type in the system catalog cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract type structure and get length field
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        int16 result = typtup->typlen;

        // Clean up cache reference and return length
        ReleaseSysCache(tp);
        return result;
    } else {
        // Type not found, return 0
        return 0;
    }
}
```