# ginVacuumPostingTree

## Location
[src/backend/access/gin/ginvacuum.c:409-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L409-L455)

## Overview
A static function that performs vacuum operations on posting trees in GIN indexes by identifying and deleting empty pages to reclaim storage space.

## Definition
```c
static void ginVacuumPostingTree(GinVacuumState *gvs, BlockNumber rootBlkno)
```

## Detailed Description
This function implements the second phase of GIN posting tree vacuum operations. It first calls ginVacuumPostingTreeLeaves to scan leaf pages and identify empty pages that can be deleted. If empty pages are found, it performs a complete rescan of the posting tree to safely delete these pages while maintaining tree integrity.

The function uses a cleanup lock on the root page to prevent concurrent insertions during the deletion process, ensuring consistency. It builds a deletion stack structure to track pages that need to be removed and uses ginScanToDelete to perform the actual deletion operations.

## Parameters / Member Variables
- `gvs`: Pointer to GinVacuumState structure containing vacuum operation context and statistics
- `rootBlkno`: Block number of the posting tree root page to be vacuumed

## Dependencies
- Functions called/Symbols referenced:
  - [ginVacuumPostingTreeLeaves](ginVacuumPostingTreeLeaves.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [LockBufferForCleanup](../L/LockBufferForCleanup.md)
  - [ginScanToDelete](ginScanToDelete.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [DataPageDeleteStack](../D/DataPageDeleteStack.md)
  - [GinVacuumState](../G/GinVacuumState.md)
- Called from (representative examples):
  - [ginbulkdelete](ginbulkdelete.md)

## Notes and Other Information
- This is a static function used internally within the GIN vacuum implementation
- Requires cleanup lock on the root page to prevent concurrent modifications
- Only performs deletion scan if ginVacuumPostingTreeLeaves identifies empty pages
- Uses a two-phase approach: first identify empty pages, then delete them in a separate scan
- Properly manages memory by freeing the deletion stack after use
- Part of the larger GIN index maintenance and space reclamation system