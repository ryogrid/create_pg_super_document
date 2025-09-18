# heapam_relation_copy_data

## Location
src/backend/access/heap/heapam_handler.c: 633 - 685

## Overview
This function creates a complete physical copy of a heap relation to a new file location, including all forks, while properly handling WAL logging and buffer management.

## Definition
static void heapam_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)

## Detailed Description
heapam_relation_copy_data performs a comprehensive physical copy of a heap relation from its current location to a new RelFileLocator. The function handles the entire copying process including flushing buffers, creating storage for the destination, copying all existing forks (main fork plus any additional forks like FSM, VM, etc.), proper WAL logging for permanent relations, and cleanup of the old storage. This operation is typically used during relation rewrites or major structural changes where the relation needs to be physically relocated.

## Parameters / Member Variables
- `rel`: The source relation to be copied
- `newrlocator`: Pointer to the RelFileLocator specifying the destination location for the copied relation

## Dependencies
- Functions called/Symbols referenced:
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)
  - [RelationCreateStorage](../R/RelationCreateStorage.md)  
  - [RelationCopyStorage](../R/RelationCopyStorage.md)
  - RelationGetSmgr
  - [smgrexists](../s/smgrexists.md)
  - [smgrcreate](../s/smgrcreate.md)
  - RelationIsPermanent
  - [log_smgrcreate](../l/log_smgrcreate.md)
  - [RelationDropStorage](../R/RelationDropStorage.md)
  - [smgrclose](../s/smgrclose.md)
- Constants referenced:
  - MAIN_FORKNUM
  - MAX_FORKNUM
  - INIT_FORKNUM
  - RELPERSISTENCE_UNLOGGED
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (referenced in heapam_handler.c:2630)

## Notes and Other Information
- This is a static function, only accessible within heapam_handler.c
- The function assumes exclusive lock is held on the relation to prevent concurrent modifications
- All shared buffers for the source relation are flushed before copying to ensure data consistency
- The function copies all existing forks, not just the main fork
- WAL logging is performed for permanent relations and init forks of unlogged relations
- The old relation storage is dropped after successful copying
- Any conflicts in relfilenumber values are caught by RelationCreateStorage()