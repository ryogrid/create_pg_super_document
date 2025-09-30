# CreateAndCopyRelationData

## Location
[src/backend/storage/buffer/bufmgr.c:4771-4834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4771-L4834)

## Overview
CreateAndCopyRelationData creates destination relation storage and copies all forks from a source relation to the destination, handling both permanent and unlogged relations appropriately.

## Definition
```c
void CreateAndCopyRelationData(RelFileLocator src_rlocator,
                              RelFileLocator dst_rlocator, 
                              bool permanent)
```

## Detailed Description
This function provides a high-level interface for complete relation duplication by creating the destination storage and copying all existing forks from source to destination. The operation includes:

- Creating the destination relation storage with appropriate persistence characteristics
- Copying the main fork using buffer manager APIs for efficiency
- Iterating through all possible fork types to copy those that exist in the source
- Creating and WAL-logging additional forks as needed
- Handling WAL logging decisions based on relation permanence and fork type

The function is designed for use in database creation operations where complete relation duplication is required.

## Parameters / Member Variables
- `src_rlocator`: RelFileLocator identifying the source relation to copy from
- `dst_rlocator`: RelFileLocator identifying the destination relation to create and copy to
- `permanent`: Boolean indicating if the relation is permanent (true) or unlogged (false) - affects persistence characteristics and WAL logging

## Dependencies
- Functions called/Symbols referenced:
  - [smgropen](../s/smgropen.md)
  - [RelationCreateStorage](../R/RelationCreateStorage.md)
  - [RelationCopyStorageUsingBuffer](../R/RelationCopyStorageUsingBuffer.md)
  - [smgrexists](../s/smgrexists.md), smgrcreate
  - [log_smgrcreate](../l/log_smgrcreate.md)
  - RELPERSISTENCE_PERMANENT, RELPERSISTENCE_UNLOGGED
  - MAIN_FORKNUM, MAX_FORKNUM, INIT_FORKNUM
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](CreateDatabaseUsingWalLog.md)

## Notes and Other Information
- This is a public function (not static) providing a high-level API for relation copying
- Currently not supported for temporary relations - only permanent and unlogged relations
- Uses RelationCreateStorage with cleanup registration disabled since database creation has its own cleanup mechanism
- WAL logging for fork creation follows the same rules as the copy operation: always for permanent relations, only for init fork of unlogged relations
- The function systematically checks all possible fork numbers beyond the main fork to ensure complete relation duplication
- Fork creation and copying are separate operations to maintain proper storage manager state

## Simplified Source

```c
void
CreateAndCopyRelationData(RelFileLocator src_rlocator,
                         RelFileLocator dst_rlocator, bool permanent)
{
    char relpersistence;
    SMgrRelation src_rel;
    SMgrRelation dst_rel;

    // Set persistence type
    relpersistence = permanent ?
        RELPERSISTENCE_PERMANENT : RELPERSISTENCE_UNLOGGED;

    src_rel = smgropen(src_rlocator, INVALID_PROC_NUMBER);
    dst_rel = smgropen(dst_rlocator, INVALID_PROC_NUMBER);

    // Create destination storage (no cleanup registration needed for database creation)
    RelationCreateStorage(dst_rlocator, relpersistence, false);

    // Copy main fork
    RelationCopyStorageUsingBuffer(src_rlocator, dst_rlocator, MAIN_FORKNUM, permanent);

    // Copy additional forks that exist
    for (ForkNumber forkNum = MAIN_FORKNUM + 1; forkNum <= MAX_FORKNUM; forkNum++) {
        if (smgrexists(src_rel, forkNum)) {
            smgrcreate(dst_rel, forkNum, false);

            // WAL log creation for permanent relations or init fork of unlogged relations
            if (permanent || forkNum == INIT_FORKNUM)
                log_smgrcreate(&dst_rlocator, forkNum);

            // Copy fork data block by block
            RelationCopyStorageUsingBuffer(src_rlocator, dst_rlocator, forkNum, permanent);
        }
    }
}
```