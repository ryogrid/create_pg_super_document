# GinVacuumState

## Location
src/backend/access/gin/ginvacuum.c: 28 - 47

## Overview
GinVacuumState is a structure that maintains the state and context information during GIN (Generalized Inverted Index) vacuum operations, encapsulating all necessary data for efficient garbage collection and space reclamation.

## Definition


## Detailed Description
The GinVacuumState structure serves as a central state container for GIN index vacuum operations. It holds references to the index being vacuumed, result tracking structures, callback functions for tuple deletion decisions, and various operational contexts. This structure is passed between different vacuum functions to maintain consistency and share state throughout the vacuum process. The structure supports both bulk delete operations and general vacuum cleanup, providing the necessary context for efficient memory management and I/O operations during the vacuum process.

## Parameters / Member Variables
- : The GIN index relation being vacuumed
- : Pointer to IndexBulkDeleteResult structure for tracking vacuum statistics and results
- : Function pointer to IndexBulkDeleteCallback for determining which tuples to delete
- : Opaque state data passed to the callback function
- : GIN-specific state information including access method details
- : Buffer access strategy for controlling buffer pool usage during vacuum
- : Temporary memory context for allocations during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)
  - IndexBulkDeleteCallback
  - [GinState](GinState.md)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md)
- Called from (representative examples):
  - [ginVacuumPostingTreeLeaf](../g/ginVacuumPostingTreeLeaf.md)
  - [ginVacuumItemPointers](../g/ginVacuumItemPointers.md)
  - [ginDeletePage](../g/ginDeletePage.md)
  - [ginScanToDelete](../g/ginScanToDelete.md)
  - [ginVacuumPostingTreeLeaves](../g/ginVacuumPostingTreeLeaves.md)
  - [ginVacuumPostingTree](../g/ginVacuumPostingTree.md)
  - [ginVacuumEntryPage](../g/ginVacuumEntryPage.md)
  - [ginbulkdelete](../g/ginbulkdelete.md)

## Notes and Other Information
This structure is fundamental to the GIN vacuum implementation, providing a clean interface for passing vacuum state between different levels of the GIN index hierarchy. The structure supports memory management through the tmpCxt field and allows for strategic buffer access control, which is crucial for vacuum performance on large indexes. The callback mechanism enables flexible tuple deletion policies during vacuum operations.