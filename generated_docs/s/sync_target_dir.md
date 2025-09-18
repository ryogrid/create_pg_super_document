# sync_target_dir

## Location
[src/bin/pg_rewind/file_ops.c:294-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L294-L313)

## Overview
Synchronizes the target PostgreSQL data directory to disk to ensure all modifications are safely persisted during pg_rewind operations.

## Definition
```c
void sync_target_dir(void)
```

## Detailed Description
This function performs a complete synchronization of the target data directory to ensure all file modifications made during the pg_rewind process are safely written to disk. It uses PostgreSQL's optimized sync_pgdata function which employs a two-pass approach when fsync is specified - first initiating writeback, then performing the actual sync. This strategy often reduces overall I/O overhead significantly. The function is designed to be called once at the end of the rewind operation for performance reasons, as the kernel likely has already flushed most dirty buffers by that point.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [sync_pgdata](sync_pgdata.md) (PostgreSQL data directory sync utility)
- Called from (representative examples):
  - [main](../m/main.md) (pg_rewind.c:526)
- Declared in:
  - file_ops.h:22

## Notes and Other Information
- Respects both the global do_sync and dry_run flags - no sync occurs if either sync is disabled or in dry-run mode
- Uses PG_VERSION_NUM and sync_method parameters when calling sync_pgdata
- Part of the performance optimization strategy in pg_rewind - single bulk sync rather than per-file syncing
- The two-pass fsync approach in sync_pgdata helps reduce I/O contention and improves overall performance
- Critical for data integrity - ensures all rewind operations are durably committed to storage