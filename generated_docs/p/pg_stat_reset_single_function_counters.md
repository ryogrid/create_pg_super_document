# pg_stat_reset_single_function_counters

## Location
[src/backend/utils/adt/pgstatfuncs.c:1761-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1761-L1771)

## Overview
A PostgreSQL system function that resets statistical counters for a single user-defined function within the current database.

## Definition


## Detailed Description
The  function provides a targeted mechanism to reset statistical counters for a specific user-defined function identified by its OID (Object Identifier). Unlike table statistics which may be shared across databases, function statistics are always scoped to the current database context, as functions are database-specific objects.

The function operates by calling the general statistics reset mechanism with the PGSTAT_KIND_FUNCTION category, the current database ID, and the specified function OID. This clears all accumulated statistics for the target function, including call counts, execution times, and other performance metrics.

## Parameters / Member Variables
-  (Oid): The Object Identifier of the function whose statistics should be reset. This parameter is required and obtained from the first function argument.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_reset](pgstat_reset.md) (to perform the actual statistics reset)
  - PG_RETURN_VOID (to return from the function)
- Constants used:
  - PGSTAT_KIND_FUNCTION (specifies that function statistics are being reset)
  - MyDatabaseId (current database identifier, as functions are database-specific)
- Called from:
  - SQL function interface (no direct C callers found)

## Notes and Other Information
- This function is specifically designed for user-defined functions, as PostgreSQL tracks statistics on function calls and performance
- Function statistics are always database-scoped, unlike some table statistics which can be cluster-wide for shared relations
- The function requires appropriate privileges to execute, as it affects function-level statistics
- Statistics typically tracked for functions include number of calls, total execution time, and average execution time per call
- The OID parameter must correspond to a valid function; invalid OIDs will be handled by the underlying pgstat_reset function
- This function is part of PostgreSQL's comprehensive statistics monitoring system that helps database administrators optimize performance