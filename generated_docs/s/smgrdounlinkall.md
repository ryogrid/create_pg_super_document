# smgrdounlinkall

## Location
src/backend/storage/smgr/smgr.c: 462 - 534

## Overview
Immediately unlinks (deletes) all forks of multiple relations from storage, providing a bulk deletion operation that cannot be undone.

## Definition


## Detailed Description
This function performs immediate and irreversible deletion of all forks for multiple relations from the storage system. It is designed as a bulk operation for efficiency and should not be used during transactional operations since it cannot be undone. The function operates in several carefully ordered phases to ensure consistency and proper cleanup.

The operation sequence is:
1. Drop all buffers for the relations without writing them to disk
2. Close all forks at the storage manager level
3. Send cache invalidation messages to other backends to force them to close any dangling references
4. Delete the physical files for all forks

The isRedo parameter allows the function to handle cases during WAL replay where files might already be deleted, treating missing files as acceptable rather than errors.

## Parameters / Member Variables
- : Array of SMgrRelation pointers representing the relations to delete
- : Integer count of the number of relations in the rels array  
- : Boolean flag indicating if this is part of WAL replay, allowing missing files

## Dependencies
- Functions called/Symbols referenced:
  - [DropRelationsAllBuffers](../D/DropRelationsAllBuffers.md) (drops buffers without writing)
  - [palloc](../p/palloc.md)/pfree (memory allocation/deallocation)
  - [RelFileLocatorBackend](../R/RelFileLocatorBackend.md) (relation file locator structure)
  - [CacheInvalidateSmgr](../C/CacheInvalidateSmgr.md) (sends invalidation messages to other backends)
  - smgrsw (storage manager switch table for close and unlink operations)
  - MAX_FORKNUM (maximum fork number constant)
- Called from (representative examples):
  - [smgrDoPendingDeletes](smgrDoPendingDeletes.md) (performs pending deletion operations)
  - [DropRelationFiles](../D/DropRelationFiles.md) (drops relation files)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md) (changes relation file numbers)
  - SmgrIsTemp (via header inclusion)

## Notes and Other Information
- Irreversible operation that should only be used outside of transactions
- Includes careful ordering: buffer drops, file closes, cache invalidation, then physical deletion
- The cache invalidation step prevents other backends from accessing stale references
- Storage manager unlink operations treat deletion failures as warnings, not errors
- Essential for relation drops, truncations, and other storage cleanup operations
- Part of PostgreSQL's storage management and cleanup infrastructure
- Located in src/backend/storage/smgr/smgr.c:462-534