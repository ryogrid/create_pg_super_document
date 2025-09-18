# pg_log_standby_snapshot

## Location
[src/backend/access/transam/xlogfuncs.c:201-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L201-L231)

## Overview
Forces logging of a standby snapshot to the WAL, enabling consistent point-in-time recovery for standby servers.

## Definition


## Detailed Description
The  function provides a mechanism to explicitly log a standby snapshot to the Write-Ahead Log. This snapshot contains information about running transactions at a specific point in time, which is crucial for maintaining consistency on standby servers during replication and recovery operations.

The function performs several validation checks before executing:
1. Ensures the database is not in recovery mode (cannot be run on standby servers)
2. Verifies that WAL level is set to "replica" or higher, which is required for standby snapshot logging
3. Calls the core  function to perform the actual snapshot logging
4. Returns the LSN where the snapshot was logged

This function is particularly useful in replication scenarios where administrators need to ensure standby servers have consistent snapshot information for point-in-time recovery operations.

## Parameters / Member Variables
- No parameters (uses  macro for SQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XLogStandbyInfoActive
  - LogStandbySnapshot
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible through the GRANT system for permission management
- Cannot be executed during database recovery - will raise an error if attempted on standby servers
- Requires wal_level to be set to "replica" or higher; will raise an error if wal_level is insufficient
- The function returns the LSN where the standby snapshot was logged, which can be useful for replication coordination
- Essential for maintaining transaction consistency across primary-standby server configurations
- Part of PostgreSQL's replication and recovery infrastructure located in src/backend/access/transam/xlogfuncs.c:201-231
- Used primarily in streaming replication setups to ensure standby servers have accurate transaction visibility information