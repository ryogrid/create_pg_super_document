# DropRelationFiles

## Location
[src/backend/storage/smgr/md.c:1448-1479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1448-L1479)

## Overview
Drops (deletes) the physical files associated with multiple relations, handling both normal operation and WAL replay scenarios.

## Definition

```c
void
DropRelationFiles(RelFileLocator *delrels, int ndelrels, bool isRedo)
```
## Detailed Description
DropRelationFiles is responsible for safely removing the physical storage files of multiple database relations. The function operates by first opening storage manager relations for all target relations, optionally logging the deletion operations for WAL replay, then performing the actual file deletions through the storage manager interface. This function is critical for maintaining database consistency during relation drops and transaction rollbacks.

The function handles two distinct scenarios:
1. Normal operation: Direct deletion of relation files
2. WAL replay (isRedo=true): Deletion during recovery, which includes logging fork-specific drop operations

## Parameters / Member Variables
- `*delrels`: Array of RelFileLocator structures identifying the relations to be dropped
- `ndelrels`: Number of relations in the delrels array
- `isRedo`: Boolean flag indicating whether this is being called during WAL replay/recovery
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

## Simplified Source

```c
void DropRelationFiles(RelFileLocator *delrels, int ndelrels, bool isRedo) {
    SMgrRelation *srels;
    int i;

    // Allocate array to hold storage manager relations
    srels = palloc(sizeof(SMgrRelation) * ndelrels);

    // Open each relation and prepare for deletion
    for (i = 0; i < ndelrels; i++) {
        SMgrRelation srel = smgropen(delrels[i], INVALID_PROC_NUMBER);

        // During WAL replay, log the drop operation for each fork
        if (isRedo) {
            ForkNumber fork;
            for (fork = 0; fork <= MAX_FORKNUM; fork++)
                XLogDropRelation(delrels[i], fork);
        }

        srels[i] = srel;
    }

    // Perform atomic deletion of all relation files
    smgrdounlinkall(srels, ndelrels, isRedo);

    // Clean up: close all opened relations and free memory
    for (i = 0; i < ndelrels; i++)
        smgrclose(srels[i]);
    pfree(srels);
}
```