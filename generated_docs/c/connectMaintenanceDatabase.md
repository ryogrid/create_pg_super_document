# connectMaintenanceDatabase

## Location
[src/fe_utils/connect_utils.c:134-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/connect_utils.c#L134-L157)

## Overview
Establishes a connection to an appropriate maintenance database, with automatic fallback logic to standard PostgreSQL maintenance databases.

## Definition

```c
PGconn *
connectMaintenanceDatabase(ConnParams *cparams,
						   const char *progname, bool echo)
```
## Detailed Description
The `connectMaintenanceDatabase` function is a specialized wrapper around `connectDatabase` that implements intelligent database selection for maintenance operations. It provides a fallback mechanism when no specific database name is provided, attempting to connect to standard PostgreSQL maintenance databases in order of preference. This function is essential for PostgreSQL client utilities that need to perform administrative operations but don't have a specific target database.

The connection logic follows this sequence:
1. If a database name is explicitly specified in cparams->dbname, connect directly to that database
2. If no database name is provided, try connecting to "postgres" database first
3. If "postgres" connection fails, fall back to "template1" database
4. Return the successful connection or NULL if all attempts fail

This approach ensures compatibility across different PostgreSQL installations where maintenance databases might have different availability.

## Parameters / Member Variables
- `cparams`: Connection parameters structure containing database connection details (host, port, user, etc.)
- `progname`: Name of the calling program for error reporting and application identification
- `echo`: Boolean flag controlling whether connection attempts should be echoed to output

## Dependencies
- Functions called/Symbols referenced:
  - [connectDatabase](connectDatabase.md) (called up to 3 times with different parameters)
  - [ConnParams](../C/ConnParams.md) (parameter structure type)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck, createdb, dropdb, createuser, dropuser)
  - [cluster_all_databases](cluster_all_databases.md)
  - [reindex_all_databases](../r/reindex_all_databases.md)
  - [vacuum_all_databases](../v/vacuum_all_databases.md)

## Notes and Other Information
- Modifies the cparams->dbname field during execution to implement fallback logic
- The "postgres" database is preferred over "template1" as it's the standard maintenance database in modern PostgreSQL
- Used extensively by PostgreSQL administrative utilities that operate across multiple databases
- The function handles the common pattern where maintenance operations need a database connection but don't target a specific user database
- Located in src/fe_utils/connect_utils.c:134-157
- Part of the frontend utilities library shared across PostgreSQL client tools