# manifest_wal_range

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:87-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L87-L94)

## Overview
A linked-list data structure that represents WAL (Write-Ahead Log) ranges described in the backup manifest, used for tracking required WAL segments for backup verification and recovery.

## Definition
```c
typedef struct manifest_wal_range
{
    TimeLineID      tli;
    XLogRecPtr      start_lsn;
    XLogRecPtr      end_lsn;
    struct manifest_wal_range *next;
    struct manifest_wal_range *prev;
} manifest_wal_range;
```

## Detailed Description
The `manifest_wal_range` structure represents a contiguous range of WAL (Write-Ahead Log) records that are required for a backup to be complete and consistent. Each range is defined by a timeline ID and start/end LSN (Log Sequence Number) positions. The structure implements a doubly-linked list to allow efficient traversal and management of multiple WAL ranges that may be needed for backup verification or recovery operations.

## Parameters / Member Variables
- `tli`: Timeline ID identifying which WAL timeline this range belongs to
- `start_lsn`: Starting Log Sequence Number (LSN) for this WAL range
- `end_lsn`: Ending Log Sequence Number (LSN) for this WAL range  
- `next`: Pointer to the next WAL range in the linked list
- `prev`: Pointer to the previous WAL range in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineID (PostgreSQL timeline identifier type)
  - XLogRecPtr (PostgreSQL LSN pointer type)

- Called from (representative examples):
  - [combinebackup_per_wal_range_cb](../c/combinebackup_per_wal_range_cb.md) (src/bin/pg_combinebackup/load_manifest.c:298)
  - [verifybackup_per_wal_range_cb](../v/verifybackup_per_wal_range_cb.md) (src/bin/pg_verifybackup/pg_verifybackup.c:582)
  - finalize_manifest (src/bin/pg_combinebackup/write_manifest.c:143)
  - [parse_required_wal](../p/parse_required_wal.md) (src/bin/pg_verifybackup/pg_verifybackup.c:956)

## Notes and Other Information
This structure is crucial for PostgreSQL backup operations as it defines the exact WAL segments needed to ensure backup consistency. The doubly-linked list design allows for efficient insertion, deletion, and traversal operations when managing multiple WAL ranges. The timeline ID is essential for handling scenarios where WAL timeline switches occur due to recovery operations.