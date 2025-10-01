# index_copy_data

## Location
[src/backend/commands/tablecmds.c:15547-15603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15547-L15603)

## Overview
Copies all data and storage files from an existing relation to a new storage location, handling multiple forks and ensuring proper WAL logging for crash recovery.

## Definition

```c
enumber value will be caught in
	 * RelationCreateStorage().
	 */
	dstrel = RelationCreateStorage(newrlocator, rel->rd_rel->relpersistence, true);
```
## Detailed Description
The  function performs a complete copy of relation storage from one location to another, preserving all data forks while maintaining data integrity and consistency. This function is primarily used during tablespace operations when moving relations between different storage locations.

The function operates by first flushing any buffered pages to ensure all data is written to disk, then creates new storage at the destination location and systematically copies all existing forks (main fork plus any additional forks like FSM, VM, etc.). It handles WAL logging appropriately based on relation persistence characteristics and finally cleans up the old storage location.

## Parameters / Member Variables
- : The source relation whose data is being copied
- : The target RelFileLocator specifying where the data should be copied to

## Dependencies
- Functions called/Symbols referenced:
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)
  - [RelationCreateStorage](../R/RelationCreateStorage.md)  
  - [RelationCopyStorage](../R/RelationCopyStorage.md)
  - [RelationGetSmgr](../R/RelationGetSmgr.md)
  - [smgrexists](../s/smgrexists.md)
  - [smgrcreate](../s/smgrcreate.md)
  - [log_smgrcreate](../l/log_smgrcreate.md)
  - [RelationDropStorage](../R/RelationDropStorage.md)
  - [smgrclose](../s/smgrclose.md)
  - RelationIsPermanent
- Called from (representative examples):
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md)

## Notes and Other Information
- Requires exclusive lock on the relation to prevent concurrent modifications during copy
- Handles both permanent and unlogged relations with appropriate WAL logging
- Creates and copies all relation forks (main, FSM, VM, init) that exist in the source
- Uses direct file copying rather than tuple-by-tuple copying for performance
- Automatically schedules cleanup of old storage files after successful copy
- Part of the ALTER TABLE SET TABLESPACE infrastructure for moving relations between tablespaces

## Simplified Source

```c
static void
index_copy_data(Relation rel, RelFileLocator newrlocator)
{
    SMgrRelation dstrel;

    // Flush any buffered pages to disk
    FlushRelationBuffers(rel);

    // Create new storage at destination
    dstrel = RelationCreateStorage(newrlocator, rel->rd_rel->relpersistence, true);

    // Copy main fork
    RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM,
                        rel->rd_rel->relpersistence);

    // Copy additional forks that exist (FSM, VM, etc.)
    for (ForkNumber forkNum = MAIN_FORKNUM + 1; forkNum <= MAX_FORKNUM; forkNum++)
    {
        if (smgrexists(RelationGetSmgr(rel), forkNum))
        {
            // Create fork at destination
            smgrcreate(dstrel, forkNum, false);

            // WAL log if needed (permanent relations or unlogged init fork)
            if (RelationIsPermanent(rel) ||
                (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
                 forkNum == INIT_FORKNUM))
                log_smgrcreate(&newrlocator, forkNum);

            // Copy fork data
            RelationCopyStorage(RelationGetSmgr(rel), dstrel, forkNum,
                                rel->rd_rel->relpersistence);
        }
    }

    // Clean up: drop old storage and close new
    RelationDropStorage(rel);
    smgrclose(dstrel);
}
```