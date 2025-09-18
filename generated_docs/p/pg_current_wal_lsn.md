# pg_current_wal_lsn

## Location
[src/backend/access/transam/xlogfuncs.c:273-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L273-L293)

## Overview
Returns the current WAL write location, indicating how much WAL data has been written out to the kernel.

## Definition


## Detailed Description
The  function provides the current Write-Ahead Log write location, which represents the position up to which WAL data has been written out to the operating system kernel. This function is particularly useful for external processes that need to understand WAL visibility for archiving, replication monitoring, or backup coordination purposes.

Key characteristics of this function:
1. Returns the current write position in the WAL stream
2. The returned LSN indicates data written to the kernel but not necessarily synced to disk
3. Cannot be executed during database recovery
4. Provides the same LSN format as other WAL-related functions like 

This function is commonly used by monitoring tools, backup scripts, and replication systems to track WAL generation progress and coordinate external archiving processes.

## Parameters / Member Variables
- No parameters (uses  macro for SQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetXLogWriteRecPtr](../G/GetXLogWriteRecPtr.md)
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible through the GRANT system for permission management
- Cannot be executed during database recovery - will raise an error if attempted on standby servers
- The returned LSN represents data written to the kernel but not necessarily flushed to disk storage
- Useful for external archiving processes to determine how much WAL data is available for processing
- The LSN format matches other WAL-related functions, enabling easy comparison and coordination
- Part of PostgreSQL's WAL monitoring infrastructure located in src/backend/access/transam/xlogfuncs.c:273-293
- Commonly used by backup tools, monitoring systems, and replication management scripts to track WAL write progress
- Distinguished from sync positions - this represents write completion, not durability guarantee