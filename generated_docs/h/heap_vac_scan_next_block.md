# heap_vac_scan_next_block

## Location
[src/backend/access/heap/vacuumlazy.c:1088-1185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1088-L1185)

## Overview
heap_vac_scan_next_block determines the next block that vacuum should process, using visibility map information and optimization thresholds to skip unnecessary blocks.

## Definition

```c
static bool
heap_vac_scan_next_block(LVRelState *vacrel, BlockNumber *blkno,
						 bool *all_visible_according_to_vm)
```
## Detailed Description
heap_vac_scan_next_block implements intelligent block selection logic for vacuum operations. It operates in three distinct states:

1. **Finding Next Unskippable Block**: After processing an unskippable block or at scan start, calls find_next_unskippable_block to identify the next block that must be processed based on visibility map information.

2. **Sequential Processing**: When blocks could be skipped but the skip distance is below SKIP_PAGES_THRESHOLD, continues sequential processing to maintain OS readahead benefits and enable more frequent relfrozenxid advancement.

3. **Processing Unskippable Block**: Handles blocks that cannot be skipped due to visibility map status or vacuum requirements.

The function optimizes vacuum performance by balancing I/O efficiency (avoiding random seeks for small skips) with vacuum effectiveness (processing necessary blocks). It tracks whether any all-visible blocks are skipped to maintain proper relfrozenxid advancement safety.

## Parameters / Member Variables
- `*vacrel`: LVRelState containing vacuum state and configuration (in/out parameter)
- `*blkno`: Output parameter set to the next block number to process
- `*all_visible_according_to_vm`: Output parameter indicating if the block is all-visible per visibility map
## Dependencies
- Functions called/Symbols referenced:
  - [find_next_unskippable_block](../f/find_next_unskippable_block.md) (identifies next required block)
  - [ReleaseBuffer](../R/ReleaseBuffer.md) (buffer management)
  - SKIP_PAGES_THRESHOLD (skip optimization threshold)

- Called from (representative examples):
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (src/backend/access/heap/vacuumlazy.c:843)

## Notes and Other Information
- Returns false when no more blocks need processing (end of relation reached)
- Uses InvalidBlockNumber + 1 overflow to 0 pattern for first call initialization
- Implements SKIP_PAGES_THRESHOLD optimization to avoid discouraging OS sequential detection
- Sets vacrel->skippedallvis flag when skipping all-visible ranges to prevent unsafe relfrozenxid updates
- Manages vacrel->next_unskippable_vmbuffer for visibility map buffer lifecycle
- Maintains vacrel->current_block state for iteration tracking
- Source location: src/backend/access/heap/vacuumlazy.c:1088-1185

## Simplified Source

```c
static bool heap_vac_scan_next_block(LVRelState *vacrel, BlockNumber *blkno,
                                     bool *all_visible_according_to_vm) {
    BlockNumber next_block;

    // Get next sequential block (InvalidBlockNumber + 1 overflows to 0 on first call)
    next_block = vacrel->current_block + 1;

    // Check if we've reached the end of the relation
    if (next_block >= vacrel->rel_pages) {
        // Clean up visibility map buffer if still held
        if (BufferIsValid(vacrel->next_unskippable_vmbuffer)) {
            ReleaseBuffer(vacrel->next_unskippable_vmbuffer);
            vacrel->next_unskippable_vmbuffer = InvalidBuffer;
        }
        *blkno = vacrel->rel_pages;
        return false;  // No more blocks to process
    }

    // State 1: Find next unskippable block using visibility map
    if (next_block > vacrel->next_unskippable_block ||
        vacrel->next_unskippable_block == InvalidBlockNumber) {

        bool skipsallvis;

        // Use visibility map to find next block that must be processed
        find_next_unskippable_block(vacrel, &skipsallvis);

        // Optimization: Only skip if we can skip a significant range
        // This avoids disrupting OS sequential readahead for small skips
        if (vacrel->next_unskippable_block - next_block >= SKIP_PAGES_THRESHOLD) {
            next_block = vacrel->next_unskippable_block;
            if (skipsallvis)
                vacrel->skippedallvis = true;  // Track for relfrozenxid safety
        }
    }

    // State 2: Process sequential blocks in a range we chose not to skip
    if (next_block < vacrel->next_unskippable_block) {
        // These blocks are all-visible but we're processing them sequentially
        *blkno = vacrel->current_block = next_block;
        *all_visible_according_to_vm = true;
        return true;
    }
    // State 3: Process the unskippable block we found
    else {
        // This is the exact block that must be processed
        Assert(next_block == vacrel->next_unskippable_block);

        *blkno = vacrel->current_block = next_block;
        *all_visible_according_to_vm = vacrel->next_unskippable_allvis;
        return true;
    }
}
```