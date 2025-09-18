# write_relmap_file

## Location
[src/backend/utils/cache/relmapper.c:889-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L889-L1038)

## Overview
write_relmap_file safely writes relation mapping data to disk with full transactional support, including WAL logging, crash safety, and proper invalidation signaling.

## Definition


## Detailed Description
write_relmap_file is the core function responsible for persistently storing relation mapping changes to disk. It implements a comprehensive approach to safe file writing that includes temporary file creation, atomic renaming, WAL logging for crash recovery, shared invalidation for cache coherency, and file preservation to prevent premature deletion.

The function follows a careful sequence: validates the mapping data, calculates CRC, writes to a temporary file, optionally logs the change to WAL, atomically renames the file to its permanent location, sends invalidation messages to other backends, and marks files for preservation. All critical operations are performed within a critical section to ensure atomicity and prevent partial state in case of failures.

## Parameters / Member Variables
- : Pointer to RelMapFile structure containing the new mapping data to write
- : Boolean indicating whether to generate WAL record for this change
- : Boolean indicating whether to send shared invalidation message to other backends
- : Boolean indicating whether to mark mapped files for preservation against deletion
- : Database OID for the mapping (InvalidOid for shared mappings)
- : Tablespace OID where the mapped files reside
- : Database path string ("global" for shared relations, specific path for local relations)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode (lock verification)
  - INIT_CRC32C/COMP_CRC32C/FIN_CRC32C (CRC calculation)
  - OpenTransientFile/CloseTransientFile (file operations)
  - pgstat_report_wait_start/pgstat_report_wait_end (wait event reporting)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert/XLogFlush (WAL operations)
  - [durable_rename](../d/durable_rename.md) (atomic file replacement)
  - [CacheInvalidateRelmap](../C/CacheInvalidateRelmap.md) (cache invalidation)
  - [RelationPreserveStorage](../R/RelationPreserveStorage.md) (file preservation)
  - START_CRIT_SECTION/END_CRIT_SECTION (critical section management)
- Called from (representative examples):
  - [RelationMapCopy](../R/RelationMapCopy.md) (at src/backend/utils/cache/relmapper.c:312)
  - [RelationMapFinishBootstrap](../R/RelationMapFinishBootstrap.md) (at src/backend/utils/cache/relmapper.c:637, 639)
  - [perform_relmap_update](../p/perform_relmap_update.md) (at src/backend/utils/cache/relmapper.c:1074)
  - [relmap_redo](../r/relmap_redo.md) (at src/backend/utils/cache/relmapper.c:1133)

## Notes and Other Information
- This is a static function, only accessible within the relmapper.c file
- Requires caller to hold RelationMappingLock in exclusive mode
- Uses temporary file and atomic rename for crash safety
- Critical sections ensure operations are atomic or result in PANIC
- WAL logging enables crash recovery and replication
- Shared invalidation ensures cache coherency across backends
- File preservation prevents premature deletion by storage manager
- Used during bootstrap, normal operation, and WAL replay scenarios
- Part of PostgreSQL's transactional relation mapping infrastructure