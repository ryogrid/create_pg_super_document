# is_load_via_partition_root

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1170-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1170-L1200)

## Overview
is_load_via_partition_root is a detection function that determines whether a TABLE DATA TOC item is performing "load via partition root", where data is being loaded into an ancestor partition table rather than the nominally specified table.

## Definition

```c
static bool
is_load_via_partition_root(TocEntry *te)
```
## Detailed Description
This function implements a critical detection mechanism for PostgreSQL's partitioned table handling during restore operations. When dealing with partitioned tables, PostgreSQL sometimes redirects data loading to go through the partition root (parent table) rather than directly to individual partitions. This function detects such scenarios to prevent optimization conflicts.

The function uses a two-tier detection strategy:

1. **Modern Archive Detection**: In newer archive files, it checks for a special comment "-- load via partition root " in the TOC entry's definition field.

2. **Legacy Archive Detection**: For older archive files, it examines the COPY statement to see if the target table differs from the nominal table name. It constructs what the COPY statement should look like for direct loading and compares it with the actual COPY statement.

This detection is crucial for optimization decisions, particularly in parallel restore scenarios where TRUNCATE operations before COPY might interfere with cross-partition data movement, potentially causing deadlocks or data loss.

## Parameters / Member Variables
- : TOC entry representing the table data loading operation to be analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [fmtQualifiedId](../f/fmtQualifiedId.md) (formats schema-qualified table names for comparison)
  - [createPQExpBuffer](../c/createPQExpBuffer.md) (creates buffer for string building)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends formatted text to buffer)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (cleans up buffer memory)
  - strncmp (string comparison function)
- Called from (representative examples):
  - [restore_toc_entry](../r/restore_toc_entry.md) (to determine if TRUNCATE optimization is safe)

## Notes and Other Information
- Returns true if load-via-partition-root is detected, false otherwise
- Critical for preventing optimization conflicts in partitioned table scenarios
- Handles both modern archives (with explicit comments) and legacy archives (via COPY statement analysis)
- May give false negatives for data dumped as INSERT commands in older archives, but this is rare
- Used to avoid TRUNCATE optimization that could interfere with partition routing
- Essential for maintaining data integrity and preventing deadlocks during parallel restore of partitioned tables
- Part of PostgreSQL's sophisticated partitioned table handling in backup/restore operations