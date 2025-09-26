# xl_smgr_create

## Location
[src/include/catalog/storage_xlog.h:33-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/storage_xlog.h#L33-L37)

## Overview
A WAL record structure that represents the creation of a storage manager file in PostgreSQL's Write-Ahead Logging (WAL) system.

## Definition

```c
typedef struct xl_smgr_create
{
	RelFileLocator rlocator;
	ForkNumber	forkNum;
} xl_smgr_create;
```
## Detailed Description
The `xl_smgr_create` structure is used to record storage manager file creation operations in the WAL. When PostgreSQL creates a new relation file (table, index, etc.), it logs this operation using this record structure to ensure crash recovery can properly recreate the file if needed. This is part of PostgreSQL's durability guarantee - all structural changes to the database must be logged before they are applied.

The structure is specifically used with the XLOG_SMGR_CREATE WAL record type and is processed during WAL replay to recreate files that were created but may have been lost due to a crash or other failure.

## Parameters / Member Variables
- `rlocator`: RelFileLocator that uniquely identifies the relation file (contains tablespace, database, and relation OIDs)
- `forkNum`: ForkNumber indicating which fork of the relation is being created (MAIN, FSM, VM, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - log_smgrcreate (creates and logs the WAL record)
  - smgr_redo (processes the WAL record during recovery)
  - smgr_desc (describes the record for debugging)
  - XLogPrefetcherNextBlock (prefetches during WAL replay)
  - SummarizeSmgrRecord (summarizes the record for WAL summarization)

## Notes and Other Information
- This structure is part of the storage manager (SMGR) WAL resource manager
- Used in conjunction with XLogInsert() to write the creation record to WAL
- During recovery, the smgr_redo() function reads this structure from WAL and calls smgrcreate() to recreate the file
- The structure is designed to be compact for efficient WAL storage
- Part of PostgreSQL's crash recovery mechanism ensuring that all file system changes are recoverable