# pg_stat_get_backend_userid

## Location
src/backend/utils/adt/pgstatfuncs.c: 694 - 705

## Overview
Returns the user OID (object identifier) of the database user that a specific PostgreSQL backend process is running as.

## Definition
```c
Datum pg_stat_get_backend_userid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves the user OID for a PostgreSQL backend process identified by its process number. It accesses the shared memory backend status array to find the backend entry corresponding to the given process number and returns the st_userid field, which contains the OID of the database role/user the backend is authenticated as. This function is essential for security monitoring, auditing, and administrative purposes, allowing users to determine which database user each backend process is operating under. If no backend exists with the specified process number, the function returns NULL.

## Parameters / Member Variables
- `procNumber` (int32): The process number identifying which backend's user ID to retrieve. This is an index into the backend status array.

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_get_beentry_by_proc_number: Retrieves backend status entry by process number
  - PgBackendStatus: Structure containing backend status information including st_userid
  - PG_GETARG_INT32: Macro to extract int32 argument from function call
  - PG_RETURN_OID: Macro to return OID value from PostgreSQL function
  - PG_RETURN_NULL: Macro to return NULL from PostgreSQL function

## Notes and Other Information
- Returns NULL if the specified process number does not correspond to an active backend
- The returned value is an OID (Object Identifier) that uniquely identifies a database role/user within the PostgreSQL cluster
- This function is commonly used by system monitoring views like pg_stat_activity to display user connections
- The user OID can be used with system catalogs (like pg_authid or pg_user) to resolve the actual username
- Auxiliary processes that are not associated with a specific user may return InvalidOid (0)
- Used for security auditing to track which user account is associated with each database connection
- Located in src/backend/utils/adt/pgstatfuncs.c:694-705