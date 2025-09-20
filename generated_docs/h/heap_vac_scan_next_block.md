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
- : LVRelState containing vacuum state and configuration (in/out parameter)
- : Output parameter set to the next block number to process
- : Output parameter indicating if the block is all-visible per visibility map

## Dependencies
- Functions called/Symbols referenced:
  - [find_next_unskippable_block](../f/find_next_unskippable_block.md) (identifies next required block)
  - ReleaseBuffer (buffer management)
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