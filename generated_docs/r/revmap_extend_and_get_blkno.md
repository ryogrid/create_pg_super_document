# revmap_extend_and_get_blkno

## Location
[src/backend/access/brin/brin_revmap.c:500-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L500-L521)

## Overview
A static helper function that finds the physical reverse map block number for a given heap block, automatically extending the reverse map if the required page hasn't been allocated yet.

## Definition


## Detailed Description
This function calculates and ensures the availability of the reverse map page for a specified heap block by:
1. Using the HEAPBLK_TO_REVMAP_BLK macro to convert the heap block number to a revmap block index
2. Adding 1 to account for the metapage block (block 0 is reserved for metadata)
3. Checking if the calculated block number exceeds the current extent of allocated revmap pages
4. Extending the revmap by calling revmap_physical_extend in a loop until the target block is available
5. Including interrupt checks during the extension process to allow for query cancellation

Unlike revmap_get_blkno which returns InvalidBlockNumber for unallocated pages, this function ensures the page exists by extending the revmap as needed.

## Parameters / Member Variables
- : The BRIN reverse map structure containing index metadata and current allocation state
- : The heap block number for which to ensure a corresponding revmap page exists

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_REVMAP_BLK (macro for block number conversion)
  - [revmap_physical_extend](revmap_physical_extend.md)
  - CHECK_FOR_INTERRUPTS (macro for interrupt processing)
- Called from (representative examples):
  - [brinRevmapExtend](../b/brinRevmapExtend.md)

## Notes and Other Information
- This is a static function, only accessible within the brin_revmap.c file
- Always returns a valid block number, unlike revmap_get_blkno which may return InvalidBlockNumber
- Extends the revmap incrementally, one page at a time, until the target block is covered
- Includes interrupt handling to allow long extension operations to be cancelled
- Essential for operations that need to create new summary tuples for previously unsummarized ranges
- The extension process involves physical allocation of new index pages and updating metadata
- Used during index maintenance operations that require guaranteed revmap coverage