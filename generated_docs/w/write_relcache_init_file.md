# write_relcache_init_file

## Location
[src/backend/utils/cache/relcache.c:6491-6702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6491-L6702)

## Overview
Writes out a new initialization file containing the current contents of the relation cache, enabling fast startup for subsequent backend processes.

## Definition

```c
static void
write_relcache_init_file(bool shared)
```
## Detailed Description
This function creates a binary initialization file containing pre-built relation cache entries to optimize backend startup performance. It writes either shared catalog relations or local database relations based on the shared parameter.

The function implements a safe write strategy using temporary files to prevent corruption if another backend attempts to read during the write process. It first writes to a temporary file with the process ID appended, then atomically renames it to the final filename.

The function validates that no relcache invalidation messages have been received during the write process, ensuring data consistency. If invalidations are detected, the temporary file is deleted rather than installed, leaving initialization to a future backend.

For each qualifying relation, the function writes the complete relation descriptor including tuple descriptors, attribute information, access method options, and for indexes, additional metadata like operator families, support procedures, collations, and index options.

The function uses write_item() as a helper to write individual data structures with their sizes to the binary file format.

## Parameters / Member Variables
- : Boolean flag indicating whether to write shared catalog relations (true) or local database relations (false)

## Dependencies
- Functions called/Symbols referenced:
  - [write_item](write_item.md)
  - AllocateFile/FreeFile
  - [RelationIdIsInInitFile](../R/RelationIdIsInInitFile.md)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - LWLockAcquire/LWLockRelease
- Called from (representative examples):
  - RelationCacheInitializePhase3 (inferred from context)
  - Relcache invalidation handlers

## Notes and Other Information
- Uses magic number RELCACHE_INIT_FILEMAGIC for file format identification
- Implements atomic file replacement using temporary files and rename()
- Checks for relcache invalidation messages throughout the process via relcacheInvalsReceived
- Uses RelCacheInitLock for serialization during final validation and file installation
- File naming convention includes process ID for temporary files to avoid conflicts
- Filters relations based on RelationIdIsInInitFile() for local databases
- Complex index metadata is fully preserved including operator classes and support functions
- File location: src/backend/utils/cache/relcache.c:6491-6702