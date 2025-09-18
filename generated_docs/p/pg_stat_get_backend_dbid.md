# pg_stat_get_backend_dbid

## Location
src/backend/utils/adt/pgstatfuncs.c: 681 - 693

## Overview
Returns the database OID (object identifier) of the database that a specific PostgreSQL backend process is connected to.

## Definition
```c
Datum pg_stat_get_backend_dbid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the database OID for a PostgreSQL backend process identified by its process number. It accesses the shared memory backend status array to find the backend entry corresponding to the given process number and returns the st_databaseid field, which contains the OID of the database the backend is connected to. This function is essential for monitoring and administrative purposes, allowing users to determine which database each backend process is working with. If no backend exists with the specified process number, the function returns NULL.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying which backend's database ID to retrieve. This is an index into the backend status array.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_beentry_by_proc_number](pgstat_get_beentry_by_proc_number.md): Retrieves backend status entry by process number
  - [PgBackendStatus](../P/PgBackendStatus.md): Structure containing backend status information including st_databaseid
  - PG_GETARG_INT32: Macro to extract int32 argument from function call
  - PG_RETURN_OID: Macro to return OID value from PostgreSQL function
  - PG_RETURN_NULL: Macro to return NULL from PostgreSQL function

## Notes and Other Information
- Returns NULL if the specified process number does not correspond to an active backend
- The returned value is an OID (Object Identifier) that uniquely identifies a database within the PostgreSQL cluster
- This function is commonly used by system monitoring views like pg_stat_activity to display database connections
- The database OID can be used with system catalogs (like pg_database) to resolve the actual database name
- Auxiliary processes that are not connected to a specific database may return InvalidOid (0)
- Located in src/backend/utils/adt/pgstatfuncs.c:681-693