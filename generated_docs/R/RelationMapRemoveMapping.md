# RelationMapRemoveMapping

## Location
src/backend/utils/cache/relmapper.c: 438 - 467

## Overview
A public function that removes a relation's mapping entry from the active local updates map, specifically designed for backing out transient mappings during operations like VACUUM FULL and CLUSTER.

## Definition


## Detailed Description
The  function is responsible for removing a specific relation mapping from the active local updates map. This function is crucial for operations that create temporary relation mappings which need to be rolled back, particularly during VACUUM FULL and CLUSTER operations on mapped relations. It searches through the active_local_updates map for the specified relation ID and removes the mapping entry by collapsing the array to maintain contiguous storage. The function enforces that only "active" (uncommitted) local mappings can be removed, making it safe for transactional operations.

## Parameters / Member Variables
- : The OID of the relation whose mapping should be removed from the active local updates map

## Dependencies
- Functions called/Symbols referenced:
  - RelMapFile (structure)
  - active_local_updates (global variable)
  - elog (for error reporting)
- Called from (representative examples):
  - finish_heap_swap
  - MinSizeOfRelmapUpdate (referenced in header)

## Notes and Other Information
- This is a public function (no static qualifier), making it accessible from other parts of PostgreSQL
- Only operates on the active_local_updates map, not on committed mappings, ensuring transactional safety
- Uses array collapse technique for removal: moves the last element to the removed position and decrements the count
- Essential for VACUUM FULL/CLUSTER operations which create temporary file mappings that must be cleaned up
- Throws an error if the mapping is not found, indicating a programming error or inconsistent state
- Part of PostgreSQL's transactional relation mapping system that supports rollback of mapping changes