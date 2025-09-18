# spgxlogVacuumLeaf

## Location
src/include/access/spgxlog.h: 201 - 221

## Overview
The spgxlogVacuumLeaf struct is a PostgreSQL WAL record structure used to log vacuum operations on SP-GiST leaf pages, which removes dead tuples and reorganizes the page structure during index maintenance.

## Definition


## Detailed Description
This structure represents a WAL record for SP-GiST leaf page vacuum operations. During vacuum, the system needs to clean up dead tuples, compact the page, and maintain proper chaining relationships between tuples. The vacuum process involves multiple operations: marking tuples as dead, creating placeholders for maintaining tuple chain integrity, moving tuples to eliminate fragmentation, and updating the chain links that connect related tuples. This struct captures all these operations so they can be replayed during WAL recovery.

## Parameters / Member Variables
- : Number of tuples that should be marked as DEAD during the vacuum operation
- : Number of tuples that should be converted to PLACEHOLDER status to maintain chain integrity
- : Number of tuples that need to be physically moved to compact the page
- : Number of tuples that require chain link updates (nextOffset modifications)
- : SP-GiST state information containing transaction ID and build flag for the source page
- : Flexible array member containing variable-length data with six arrays: dead tuple numbers, placeholder tuple numbers, source locations for moves, destination locations for moves, tuple numbers needing chain updates, and new chain link values

## Dependencies
- Functions called/Symbols referenced:
  - spgxlogState
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - vacuumLeafPage (src/backend/access/spgist/spgvacuum.c:129)
  - spgRedoVacuumLeaf (src/backend/access/spgist/spgxlog.c:755)
  - spg_desc (src/backend/access/rmgrdesc/spgdesc.c:102)
  - SizeOfSpgxlogVacuumLeaf (src/include/access/spgxlog.h:223)

## Notes and Other Information
- This is part of the SP-GiST vacuum system which maintains index efficiency by removing dead space and reorganizing pages
- The variable data section contains six sequential arrays that must be processed in the correct order during redo operations
- Placeholder tuples are used to maintain chain integrity when the actual tuple data is no longer needed but the chain structure must be preserved
- The chain operations (nChain) handle the nextOffset links that form chains of tuples within SP-GiST leaf pages
- Proper handling of this record is crucial for maintaining SP-GiST index consistency during crash recovery