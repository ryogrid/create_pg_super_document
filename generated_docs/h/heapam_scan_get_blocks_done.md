# heapam_scan_get_blocks_done

## Location
[src/backend/access/heap/heapam_handler.c:1995-2039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1995-L2039)

## Overview
Returns the number of blocks read by a heap scan since starting, handling both serial and parallel scan scenarios for progress reporting.

## Definition
```c
static BlockNumber heapam_scan_get_blocks_done(HeapScanDesc hscan)
```

## Detailed Description
This function calculates the number of blocks that have been processed during a heap scan operation. It handles two scan types: serial scans and parallel scans. For parallel scans, it extracts information from the ParallelBlockTableScanDesc structure, while for serial scans it uses the heap scan descriptor directly. The function accounts for potential wraparound scenarios where scanning may continue from block 0 after reaching the end of the relation, which can occur when the start block is not zero.

The calculation considers the current block position (rs_cblock) relative to the starting block position. If the current block is ahead of the start block, it simply returns the difference. If wraparound has occurred (current block is behind start block numerically), it calculates the total by adding the blocks from start to end plus the blocks from beginning to current position.

## Parameters / Member Variables
- `hscan`: Heap scan descriptor containing scan state and position information

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelBlockTableScanDesc](../P/ParallelBlockTableScanDesc.md) (struct access)
  - [HeapScanDesc](../H/HeapScanDesc.md) (struct access)
- Called from (representative examples):
  - [heapam_index_build_range_scan](heapam_index_build_range_scan.md)

## Notes and Other Information
This function is primarily intended for progress reporting during index builds and other long-running scan operations. The accuracy note indicates that in parallel scenarios, workers may be reading blocks concurrently, so the returned value represents an approximation rather than an exact count. The wraparound handling is crucial for scans that use synchronized scanning or start from arbitrary positions within the relation.

## Simplified Source

```c
static BlockNumber heapam_scan_get_blocks_done(HeapScanDesc hscan) {
    ParallelBlockTableScanDesc bpscan = NULL;
    BlockNumber startblock;
    BlockNumber blocks_done;

    // Get starting block - from parallel scan or regular scan
    if (hscan->rs_base.rs_parallel != NULL) {
        bpscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
        startblock = bpscan->phs_startblock;
    } else {
        startblock = hscan->rs_startblock;
    }

    // Calculate blocks done, handling potential wraparound
    if (hscan->rs_cblock > startblock) {
        // Normal case: current block is ahead of start
        blocks_done = hscan->rs_cblock - startblock;
    } else {
        // Wraparound case: scan went from start to end, then from 0 to current
        BlockNumber nblocks = bpscan ? bpscan->phs_nblocks : hscan->rs_nblocks;
        blocks_done = nblocks - startblock + hscan->rs_cblock;
    }

    return blocks_done;
}
```