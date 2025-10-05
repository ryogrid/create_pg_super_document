# pg_tablespace_size_oid

## Location
[src/backend/utils/adt/dbsize.c:272-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L272-L285)

## Overview
A PostgreSQL system function that returns the total disk space used by the specified tablespace identified by its OID.

## Definition
```c
Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a SQL-callable interface to determine the physical size of a tablespace by its object identifier (OID). It acts as a wrapper around the `calculate_tablespace_size` function, handling the PostgreSQL function call interface and return value formatting. The function calculates the total space consumed by all databases and objects stored within the specified tablespace.

When the tablespace size calculation fails or encounters an error, the function returns NULL instead of raising an exception, providing a safe way to query tablespace sizes even for potentially problematic tablespaces.

## Parameters / Member Variables
- `tblspcOid`: OID of the tablespace whose size is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [calculate_tablespace_size](../c/calculate_tablespace_size.md)
  - PG_RETURN_INT64
  - PG_RETURN_NULL
  - PG_GETARG_OID
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is part of PostgreSQL's system administration functions accessible via SQL
- Returns NULL if size calculation fails (size < 0), providing error-safe behavior
- The function is defined in src/backend/utils/adt/dbsize.c:272-285
- Typically used in conjunction with system catalogs to provide tablespace size information to administrators
- The underlying calculation includes all databases and objects within the tablespace directory structure

## Simplified Source

```c
Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS) {
    Oid tblspcOid = PG_GETARG_OID(0);
    int64 size;

    // Calculate the tablespace size using internal function
    size = calculate_tablespace_size(tblspcOid);

    // Return NULL if calculation failed (size < 0)
    if (size < 0)
        PG_RETURN_NULL();

    // Return the size in bytes
    PG_RETURN_INT64(size);
}
```