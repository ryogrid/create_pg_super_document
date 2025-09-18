# pgstat_write_statsfile

## Location
[src/backend/utils/activity/pgstat.c:1310-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1310-L1478)

## Overview
Writes all PostgreSQL statistics data to a persistent file on disk, typically called during server shutdown to preserve statistics across restarts.

## Definition


## Detailed Description
This function is responsible for persisting the entire PostgreSQL statistics subsystem state to disk. It writes statistics data to a temporary file first, then atomically renames it to the permanent location to ensure data integrity. The function handles both fixed-format statistics (like archiver, bgwriter, checkpointer stats) and variable entries (databases, tables, functions, etc.) from the shared hash table.

The writing process includes:
1. Opening a temporary statistics file
2. Writing a format identifier header
3. Writing all fixed-format statistics snapshots for different subsystems
4. Iterating through the shared hash table to write all dynamic statistics entries
5. Closing the file and atomically renaming it to the permanent location

The function uses deferred error checking - individual write operations don't check for errors immediately, but  is called at the end to detect any write failures. This approach is more efficient for bulk write operations.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - : Verifies statistics subsystem is operational
  - : Opens the temporary statistics file
  -  and : Helper functions for writing data
  - : Builds snapshots for fixed statistics kinds
  - : Gets metadata about statistics kinds
  - : Iterates through shared hash table entries
  - : Gets statistics entry data and size
  - File system functions: , , 

- Called from (representative examples):
  - : During server shutdown process

## Notes and Other Information
- This function is called only during server shutdown when no locking is required
- Uses atomic file replacement (write to temp, then rename) for data safety
- Writes both fixed-format statistics and dynamic hash table entries
- Includes comprehensive error handling with detailed logging
- The function sets  to NONE during shutdown
- Statistics entries marked as 'dropped' are skipped during writing
- The file format includes type indicators ('S' for standard entries, 'N' for named entries, 'E' for end)
- Comments suggest the function could be generalized to iterate over  instead of hardcoding statistics types