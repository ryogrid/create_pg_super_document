# RelationCacheInitFilePreInvalidate

## Location
[src/backend/utils/cache/relcache.c:6766-6790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6766-L6790)

## Overview
Invalidates (removes) the relation cache initialization files during commit of a transaction that modified relations stored in the init files, ensuring cache consistency.

## Definition

```c
struct dirent *de;
```
## Detailed Description
This function is part of a two-phase process for invalidating relation cache initialization files when transactions modify system catalogs. It performs the first phase: acquiring the serialization lock and removing obsolete initialization files.

The function implements a carefully designed protocol to ensure consistency between concurrent processes. It acquires RelCacheInitLock exclusively to serialize against other processes that might be reading or writing initialization files. This prevents race conditions where a stale initialization file could be installed after invalidation.

The function removes both local (database-specific) and shared (global) initialization files. The removal happens before sending invalidation messages (which occurs between this function and RelationCacheInitFilePostInvalidate), ensuring that starting backends cannot read stale files and then miss the invalidation messages.

The design handles the case where files might not exist (if no backend has started since the last removal) by using unlink_initfile() which only complains about errors other than ENOENT.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [unlink_initfile](../u/unlink_initfile.md)
  - LWLockAcquire (RelCacheInitLock)
  - RELCACHE_INIT_FILENAME (constant)
  - DatabasePath (global variable)
- Called from (representative examples):
  - [ProcessCommittedInvalidationMessages](../P/ProcessCommittedInvalidationMessages.md)
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
- Must be paired with RelationCacheInitFilePostInvalidate to complete the invalidation process
- Caller must send pending SI (shared invalidation) messages between Pre and Post calls
- Uses ERROR level for file removal failures (except ENOENT), allowing transaction abort
- Part of the transaction commit protocol for maintaining relcache consistency
- The lock prevents concurrent write_relcache_init_file from installing stale data
- Handles both local database init files and shared catalog init files
- File location: src/backend/utils/cache/relcache.c:6766-6790