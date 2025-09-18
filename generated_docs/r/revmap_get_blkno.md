# revmap_get_blkno

## Location
src/backend/access/brin/brin_revmap.c: 442 - 462

## Overview
A static helper function that computes the physical block number of the reverse map page corresponding to a given heap block number.

## Definition


## Detailed Description
This function calculates which reverse map page contains the mapping information for a specified heap block by:
1. Using the HEAPBLK_TO_REVMAP_BLK macro to convert the heap block number to a revmap block index
2. Adding 1 to skip the metapage block (block 0 is reserved for metadata)
3. Checking if the calculated block number is within the allocated range of revmap pages
4. Returning InvalidBlockNumber if the revmap page hasn't been allocated yet

The function provides a simple mapping from heap block addresses to their corresponding reverse map storage locations, which is fundamental to BRIN index operations.

## Parameters / Member Variables
- : The BRIN reverse map structure containing index metadata and page range information
- : The heap block number for which to find the corresponding revmap page

## Dependencies
- Functions called/Symbols referenced:
  - HEAPBLK_TO_REVMAP_BLK (macro for block number conversion)
- Called from (representative examples):
  - [brinGetTupleForHeapBlock](../b/brinGetTupleForHeapBlock.md)
  - [brinRevmapDesummarizeRange](../b/brinRevmapDesummarizeRange.md)
  - [revmap_get_buffer](revmap_get_buffer.md)

## Notes and Other Information
- This is a static function, only accessible within the brin_revmap.c file
- Returns InvalidBlockNumber when the requested revmap page has not been allocated yet
- The calculation accounts for the metapage by adding 1 to the computed block number
- Critical for determining whether a heap range has been summarized in the BRIN index
- Used as a building block for higher-level revmap operations like tuple fetching and desummarization