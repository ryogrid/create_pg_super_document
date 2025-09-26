# BlockRefTableEntryGetBlocks

## Location
[src/common/blkreftable.c:369-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L369-L473)

## Overview
Extracts modified block numbers from a block reference table entry within a specified range, handling both bitmap and offset array storage formats.

## Definition
```c
int BlockRefTableEntryGetBlocks(BlockRefTableEntry *entry,
                               BlockNumber start_blkno,
                               BlockNumber stop_blkno,
                               BlockNumber *blocks,
                               int nblocks)
```

## Detailed Description
This function retrieves block numbers that have been marked as modified within a BlockRefTableEntry, filtering them to return only those that fall within the specified range [start_blkno, stop_blkno). The function handles the internal storage format transparently, whether the data is stored as a bitmap (for dense populations) or as an array of offsets (for sparse populations).

The function operates by:
1. Calculating which chunks might contain blocks within the specified range
2. Iterating through relevant chunks and examining their storage format
3. For bitmap format: testing each bit within the range
4. For offset array format: checking each stored offset against the range
5. Converting chunk-relative positions back to absolute block numbers
6. Early termination when the output buffer is full

The implementation includes careful overflow handling since stop_blkno could be InvalidBlockNumber (maximum value).

## Parameters / Member Variables
- : Pointer to the BlockRefTableEntry to read from (must not be NULL)
- : First block number to include in results (inclusive)
- : Block number to stop at (exclusive)
- : Output array to store found block numbers (must have space for nblocks)
- : Maximum number of block numbers that can be stored in blocks array

## Dependencies
- Functions called/Symbols referenced:
  - : Assertion macros for validation
  - : Constant defining chunk size
  - : Constant indicating bitmap storage mode
  - : Constant for bitmap entry size
  - : Type for chunk data storage
- Called from (representative examples):
  - : During incremental backup to determine which blocks need backing up

## Notes and Other Information
- Returns the actual number of block numbers written to the blocks array
- Handles two internal storage formats transparently: bitmap for dense block populations, offset arrays for sparse ones
- The function performs range filtering, only returning blocks where start_blkno ≤ block < stop_blkno
- Includes overflow protection when calculating chunk boundaries, important when stop_blkno is InvalidBlockNumber
- Early termination occurs when the output buffer is full, preventing buffer overruns
- Chunk-based storage allows efficient representation of both sparse and dense block modification patterns
- The bitmap format uses individual bits to represent block modification status
- The offset array format stores actual block offsets within chunks for sparse modification patterns
- This function is critical for incremental backup operations, determining exactly which blocks have been modified