# pg_replication_origin_create

## Location
[src/backend/replication/logical/origin.c:1269-1309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1269-L1309)

## Overview
SQL function that creates a new replication origin with the specified name and returns the assigned origin identifier (OID) for use in logical replication tracking.

## Definition

```c
Datum
pg_replication_origin_create(PG_FUNCTION_ARGS)
```
## Detailed Description
This is a PostgreSQL SQL function wrapper that creates a new replication origin for logical replication tracking. It provides a user-facing interface to the internal replorigin_create() functionality through the SQL command interface.

The function performs several validation steps:
1. Checks system prerequisites for replication origin operations
2. Converts the input text parameter to a C string
3. Validates that the name is not reserved ("any", "none", or starting with "pg_")
4. Creates the replication origin using the internal replorigin_create() function
5. Returns the assigned origin identifier

The function enforces naming conventions to prevent conflicts with system-reserved names and includes debugging support for regression testing when built with appropriate compiler flags.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - replorigin_check_prerequisites
  - text_to_cstring
  - [IsReservedName](../I/IsReservedName.md)
  - IsReservedOriginName
  - replorigin_create
  - PG_RETURN_OID
  - LOGICALREP_ORIGIN_ANY/LOGICALREP_ORIGIN_NONE (constants)
- Called from:
  - SQL interface (no direct C code references found)

## Notes and Other Information
- This is the SQL-callable interface for creating replication origins
- Reserved names include "any", "none", and any name starting with "pg_"
- Includes optional regression test naming enforcement when compiled with ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS
- Memory management includes proper cleanup with pfree() of converted string
- Returns the newly assigned replication origin ID which can be used in subsequent replication operations
- Part of PostgreSQL's logical replication infrastructure for tracking replication progress from multiple sources