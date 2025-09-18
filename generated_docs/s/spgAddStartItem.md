# spgAddStartItem

## Location
src/backend/access/spgist/spgscan.c: 130 - 153

## Overview
Creates and initializes the starting search item for SP-GiST scan operations, setting up the entry point for tree traversal from either the root or NULL partition.

## Definition


## Detailed Description
This function creates the initial search item that serves as the starting point for SP-GiST tree traversal. It handles both regular searches (starting from the tree root) and searches that need to include NULL values (starting from the NULL partition). The function allocates a properly initialized SpGistSearchItem, sets up the appropriate block number and offset based on whether NULL values should be searched, and adds the item to the search queue.

The function establishes the foundation for the search by creating an inner node item at level 0 with zero distances (for KNN searches). All fields are properly initialized to safe default values, ensuring consistent behavior during subsequent tree traversal operations.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing the scan context and zero distances array for initialization
- : Boolean flag indicating whether this start item should target the NULL partition (true) or the root block (false)

## Dependencies
- Functions called/Symbols referenced:
  - spgAllocSearchItem (allocates the search item structure)
  - ItemPointerSet (sets the heap pointer to appropriate block and offset)
  - spgAddSearchItemToQueue (adds the item to the search queue)
  - SPGIST_ROOT_BLKNO (constant for root block number)
  - SPGIST_NULL_BLKNO (constant for NULL partition block number)
  - FirstOffsetNumber (constant for first offset number)
- Called from (representative examples):
  - resetSpGistScanOpaque (called twice - once for root, once for NULL partition if needed)

## Notes and Other Information
- Creates inner node items (isLeaf = false) at level 0 representing tree entry points
- Uses zero distances from the scan context for proper KNN search initialization
- Conditionally targets either the root block or NULL partition based on the isnull parameter
- Essential for establishing the starting conditions for both regular and NULL-inclusive searches
- The created item becomes the first element in the search queue, driving subsequent tree traversal
- All traversal-specific fields are initialized to NULL/false for safety