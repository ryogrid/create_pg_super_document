# pg_show_replication_origin_status

## Location
[src/backend/replication/logical/origin.c:1516-1519](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1516-L1519)

## Overview
This function provides status information about all active replication origins in the PostgreSQL database, displaying their current progress state for logical replication.

## Definition

```c
Datum
pg_show_replication_origin_status(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a set-returning function (SRF) that iterates through all replication origin slots and displays their current status information. This function is the underlying implementation for the  system view. It returns information about each active replication origin including its identifier, name, remote LSN position, and local LSN position.

The function acquires shared locks to safely read replication state but does not prevent concurrent modifications, so slightly out-of-date values are possible. It only displays replication origins that are currently in use (not InvalidRepOriginId).

## Parameters / Member Variables


## Return Columns
The function returns a table with 4 columns (REPLICATION_ORIGIN_PROGRESS_COLS):
- Column 0:  (Oid) - The local replication origin identifier
- Column 1:  (text) - The external name of the replication origin
- Column 2:  (pg_lsn) - The LSN position on the remote/source system
- Column 3:  (pg_lsn) - The corresponding LSN position locally applied

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates replication origin prerequisites
  -  - Initializes set-returning function context
  - / - Lock management for thread safety
  -  - Resolves origin ID to name
  -  - Stores result tuples
- Data structures used:
  -  - Structure containing replication origin state
  -  - PostgreSQL SRF result information
- Global variables:
  -  - Array of replication state structures
  -  - Maximum number of replication slots
- Called from:
  -  system view (via system_views.sql:1354)

## Notes and Other Information
- This function requires superuser privileges or membership in  role (access is explicitly revoked from public in system_functions.sql:742)
- The function is designed to be safe for concurrent access but may show slightly stale data due to lockless reads of some fields
- Used primarily for monitoring and debugging logical replication setups
- Returns 0 rows if no replication origins are configured or active
- Part of PostgreSQL's logical replication infrastructure introduced for tracking replication progress across multiple origins
- The function is registered in the system catalog with an estimated row count of 100 (pg_proc.dat:11923)