# DropRelationFiles

## Location
src/backend/storage/smgr/md.c: 1448 - 1479

## Overview
Drops (deletes) the physical files associated with multiple relations, handling both normal operation and WAL replay scenarios.

## Definition


## Detailed Description
DropRelationFiles is responsible for safely removing the physical storage files of multiple database relations. The function operates by first opening storage manager relations for all target relations, optionally logging the deletion operations for WAL replay, then performing the actual file deletions through the storage manager interface. This function is critical for maintaining database consistency during relation drops and transaction rollbacks.

The function handles two distinct scenarios:
1. Normal operation: Direct deletion of relation files
2. WAL replay (isRedo=true): Deletion during recovery, which includes logging fork-specific drop operations

## Parameters / Member Variables
- : Array of RelFileLocator structures identifying the relations to be dropped
- : Number of relations in the delrels array
- : Boolean flag indicating whether this is being called during WAL replay/recovery

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [smgropen](../s/smgropen.md)
  - [XLogDropRelation](../X/XLogDropRelation.md)
  - [smgrdounlinkall](../s/smgrdounlinkall.md)
  - [smgrclose](../s/smgrclose.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md) (src/backend/access/transam/twophase.c:1606)
  - [xact_redo_commit](../x/xact_redo_commit.md) (src/backend/access/transam/xact.c:6177)
  - [xact_redo_abort](../x/xact_redo_abort.md) (src/backend/access/transam/xact.c:6288)

## Notes and Other Information
- The function uses temporary memory allocation (palloc/pfree) to manage the array of SMgrRelation pointers
- During WAL replay, the function iterates through all fork numbers (0 to MAX_FORKNUM) to ensure complete cleanup
- The actual file deletion is delegated to smgrdounlinkall() for atomic multi-relation deletion
- All opened storage manager relations are properly closed to avoid resource leaks
- This function is part of PostgreSQL's storage manager (md.c) and is essential for transaction cleanup and recovery operations