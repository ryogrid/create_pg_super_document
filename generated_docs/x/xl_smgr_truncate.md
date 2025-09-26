# xl_smgr_truncate

## Location
[src/include/catalog/storage_xlog.h:46-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/storage_xlog.h#L46-L51)

## Overview
A WAL record structure that represents the truncation of a storage manager file in PostgreSQL's Write-Ahead Logging (WAL) system.

## Definition
```c
typedef struct xl_smgr_truncate
{
    BlockNumber blkno;
    RelFileLocator rlocator;
    int         flags;
} xl_smgr_truncate;
```

## Detailed Description
The `xl_smgr_truncate` structure is used to record storage manager file truncation operations in the WAL. When PostgreSQL truncates a relation file (reducing its size by removing blocks from the end), it logs this operation using this record structure to ensure crash recovery can properly replay the truncation if needed. This is critical for maintaining data consistency across crashes, as truncation operations must be idempotent and recoverable.

The structure is used with the XLOG_SMGR_TRUNCATE WAL record type and is processed during WAL replay to recreate the truncated state of files that may have been lost due to a crash or other failure.

## Parameters / Member Variables
- `blkno`: BlockNumber indicating the new size of the relation (first block number to be truncated away)
- `rlocator`: RelFileLocator that uniquely identifies the relation file (contains tablespace, database, and relation OIDs)
- `flags`: Integer bitmask specifying which forks should be truncated:
  - `SMGR_TRUNCATE_HEAP` (0x0001): Truncate the main heap fork
  - `SMGR_TRUNCATE_VM` (0x0002): Truncate the visibility map fork
  - `SMGR_TRUNCATE_FSM` (0x0004): Truncate the free space map fork
  - `SMGR_TRUNCATE_ALL`: Truncate all forks (combination of the above flags)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - RelationTruncate (creates and logs the WAL record during relation truncation)
  - smgr_redo (processes the WAL record during recovery)
  - smgr_desc (describes the record for debugging)
  - XLogPrefetcherNextBlock (prefetches during WAL replay)
  - SummarizeSmgrRecord (summarizes the record for WAL summarization)

## Notes and Other Information
- This structure is part of the storage manager (SMGR) WAL resource manager
- Truncation operations are logged before the actual file system truncation occurs to ensure WAL-first rule compliance
- During recovery, the smgr_redo() function reads this structure from WAL and calls smgrtruncate2() to recreate the truncated files
- The operation is performed in a critical section to ensure atomicity
- Supports truncation of multiple relation forks (main heap, FSM, VM) in a single operation
- The flags field allows selective truncation of different fork types depending on what exists and needs truncation
- Part of PostgreSQL's crash recovery mechanism ensuring that all file system changes are recoverable and consistent