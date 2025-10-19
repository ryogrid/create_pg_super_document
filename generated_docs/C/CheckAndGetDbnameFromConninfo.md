# CheckAndGetDbnameFromConninfo

## Location
[src/backend/replication/logical/slotsync.c:1012-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1012-L1038)

## Overview
Validates and extracts the database name from the primary_conninfo configuration, ensuring it's properly specified for replication slot synchronization.

## Definition

```c
char *
CheckAndGetDbnameFromConninfo(void)
```
## Detailed Description
This function is a validation helper used in PostgreSQL's logical replication slot synchronization. It ensures that the  configuration parameter contains a valid  specification, which is required for establishing database connections during slot synchronization operations.

The function calls  to parse the connection string and extract the database name. If no database name is found in the connection info, it raises an error with a clear message indicating that the  parameter must be specified in  for slot synchronization to work properly.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  -  - Parses connection info and extracts database name
  -  - Global variable containing primary server connection information
- Called from:
  -  - Main function of replication slot sync worker (line 1432)
  -  - SQL function for manual slot synchronization (src/backend/replication/slotfuncs.c:910)
  - Referenced in  header file (src/include/replication/slotsync.h:26)

## Notes and Other Information
- Returns a newly allocated string containing the database name (caller must free)
- Raises ERROR with ERRCODE_INVALID_PARAMETER_VALUE if dbname is not specified
- Essential for slot synchronization as database connections are required for walrcv_exec operations
- Part of PostgreSQL's configuration validation for logical replication setup
- Provides clear, translatable error messages for configuration issues

## Simplified Source

```c
/*
 * Checks if dbname is specified in 'primary_conninfo'.
 * Error out if not specified otherwise return it.
 */
char *
CheckAndGetDbnameFromConninfo(void)
{
    // Extract database name from primary connection info
    char *dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);

    // Error if no database name specified
    if (dbname == NULL)
        ereport(ERROR,
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("replication slot synchronization requires \"dbname\" to be specified in \"primary_conninfo\""));

    return dbname;
}
```