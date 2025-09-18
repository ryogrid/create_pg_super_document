# pg_tablespace_size_name

## Location
src/backend/utils/adt/dbsize.c: 286 - 307

## Overview
A PostgreSQL system function that returns the total disk space used by the specified tablespace identified by its name.

## Definition
```c
Datum pg_tablespace_size_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a name-based interface to determine the physical size of a tablespace. It accepts a tablespace name as input, resolves it to the corresponding OID using `get_tablespace_oid`, and then delegates the actual size calculation to `calculate_tablespace_size`. This function serves as a more user-friendly alternative to `pg_tablespace_size_oid` by allowing administrators to query tablespace sizes using human-readable names instead of numeric OIDs.

Like its OID-based counterpart, this function returns NULL when the size calculation fails, providing safe error handling for potentially problematic tablespaces.

## Parameters / Member Variables
- `tblspcName`: Name of the tablespace whose size is to be calculated (as a PostgreSQL Name type)

## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
  - [calculate_tablespace_size](../c/calculate_tablespace_size.md)
  - PG_GETARG_NAME
  - PG_RETURN_INT64
  - PG_RETURN_NULL
  - NameStr
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is part of PostgreSQL's system administration functions accessible via SQL
- Provides a name-based interface for tablespace size queries, more user-friendly than OID-based queries
- The function performs name-to-OID resolution using `get_tablespace_oid` with `missing_ok=false`, meaning it will raise an error for non-existent tablespaces
- Returns NULL if size calculation fails (size < 0), providing error-safe behavior
- The function is defined in src/backend/utils/adt/dbsize.c:286-307
- Commonly used in administrative queries where tablespace names are more convenient than OIDs