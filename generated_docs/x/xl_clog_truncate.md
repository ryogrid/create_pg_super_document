# xl_clog_truncate

## Location
[src/include/access/clog.h:32-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/clog.h#L32-L37)

## Overview
A structure that represents the WAL (Write-Ahead Log) record data for CLOG truncation operations, used to record information needed for transaction status log truncation during WAL recovery.

## Definition


## Detailed Description
The  structure is part of PostgreSQL's CLOG (Commit Log) subsystem and is used to store information about CLOG truncation operations in the Write-Ahead Log. This structure contains the essential information needed to replay CLOG truncation during crash recovery or when processing WAL records on standby servers.

CLOG is PostgreSQL's transaction status log that tracks the commit status of transactions. When old transaction status information is no longer needed (because all transactions in those pages have been vacuumed away), the CLOG can be truncated to reclaim space. This truncation operation must be logged in the WAL to ensure consistency across primary and standby servers.

The structure is used in conjunction with the  WAL record type (0x10) and is processed by the CLOG resource manager during WAL replay operations.

## Parameters / Member Variables
- : The page number up to which the CLOG should be truncated. All CLOG pages before this page number will be removed.
- : The oldest transaction ID that still needs to be tracked in the CLOG after truncation. This represents the oldest transaction that hasn't been fully processed by vacuum.
- : The database OID associated with the oldest transaction. This helps track which database contains the oldest transaction that still needs CLOG entries.

## Dependencies
- Functions that use this structure:
  - : Creates WAL records containing this structure for CLOG truncation operations
  - : Processes WAL records containing this structure during recovery
  - : Formats this structure for debugging and logging purposes in WAL record descriptions

- Called from (representative examples):
  - : Indirectly uses this through  when performing CLOG truncation
  - WAL replay mechanisms during crash recovery and standby processing

## Notes and Other Information
- This structure is part of PostgreSQL's WAL logging system and ensures that CLOG truncation operations can be properly replayed on standby servers
- The structure size must be carefully maintained as it's serialized directly into WAL records using 
- The  field uses int64 to support large CLOG installations with many transaction pages
- Related constant  (0x10) identifies WAL records containing this structure type
- Part of the broader CLOG subsystem that tracks transaction commit/abort status in PostgreSQL