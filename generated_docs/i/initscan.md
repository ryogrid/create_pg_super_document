# initscan

## Location
[src/backend/access/heap/heapam.c:338-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L338-L465)

## Overview
A core initialization function that sets up the scan state and determines scanning strategy for both heap_beginscan and heap_rescan operations.

## Definition

```c
static void
initscan(HeapScanDesc scan, ScanKey key, bool keep_startblock)
```
## Detailed Description
initscan is a central function that initializes heap scan state and determines the optimal scanning strategy based on table size, system configuration, and scan parameters. It handles both initial scans and rescans, making strategic decisions about buffer access patterns, synchronization, and parallel processing.

Key responsibilities include:
1. **Block count determination**: Sets rs_nblocks based on whether this is a parallel scan or regular scan
2. **Strategy selection**: Decides whether to use bulk-read access strategy and synchronized scanning based on table size relative to NBuffers
3. **Parallel scan coordination**: Configures synchronization settings for parallel scans
4. **Start block determination**: Sets the starting block for synchronized scans or maintains previous position for rescans  
5. **State initialization**: Resets scan state variables and initializes scan direction
6. **Key copying**: Copies scan keys if provided
7. **Statistics tracking**: Updates heap scan statistics for sequential scans

The function applies a threshold of NBuffers/4 to determine if a table is "large" enough to benefit from bulk-read strategy and synchronized scanning, which helps optimize I/O patterns for large table scans.

## Parameters / Member Variables
- `scan`: HeapScanDesc containing the scan state and configuration to initialize
- `key`: ScanKey array containing scan keys to copy into the scan descriptor, or NULL if no keys
- `keep_startblock`: Boolean indicating whether to preserve the previous startblock setting during a rescan operation
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - RelationUsesLocalBuffers  
  - [GetAccessStrategy](../G/GetAccessStrategy.md)
  - [FreeAccessStrategy](../F/FreeAccessStrategy.md)
  - [ss_get_location](../s/ss_get_location.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - pgstat_count_heap_scan
  - [ParallelBlockTableScanDesc](../P/ParallelBlockTableScanDesc.md) (type cast)
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md) (src/backend/access/heap/heapam.c:1163)
  - [heap_rescan](../h/heap_rescan.md) (src/backend/access/heap/heapam.c:1250)

## Notes and Other Information
- The NBuffers/4 threshold for strategy decisions matches similar logic in table_block_parallelscan_initialize
- For parallel scans, synchronization settings are determined by the ParallelTableScanDesc rather than local logic
- The keep_startblock parameter ensures cursor rewinding produces consistent results by maintaining scan position
- Synchronized scanning (syncscan.c) helps multiple concurrent scans of the same table coordinate their reading patterns
- BAS_BULKREAD strategy optimizes buffer replacement for large sequential scans
- The function always initializes scan direction to ForwardScanDirection as it's the most common case
- Only sequential heap scans (SO_TYPE_SEQSCAN) increment the heap scan statistics counter

## Simplified Source

```c
static void initscan(HeapScanDesc scan, ScanKey key, bool keep_startblock) {
    ParallelBlockTableScanDesc bpscan = NULL;
    bool allow_strat, allow_sync;

    // Determine number of blocks to scan
    if (scan->rs_base.rs_parallel != NULL) {
        bpscan = (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
        scan->rs_nblocks = bpscan->phs_nblocks;
    } else {
        scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_base.rs_rd);
    }

    // Enable bulk-read strategy and sync scanning for large tables
    if (!RelationUsesLocalBuffers(scan->rs_base.rs_rd) &&
        scan->rs_nblocks > NBuffers / 4) {
        allow_strat = (scan->rs_base.rs_flags & SO_ALLOW_STRAT) != 0;
        allow_sync = (scan->rs_base.rs_flags & SO_ALLOW_SYNC) != 0;
    } else {
        allow_strat = allow_sync = false;
    }

    // Set up bulk-read access strategy
    if (allow_strat) {
        if (scan->rs_strategy == NULL)
            scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
    } else {
        if (scan->rs_strategy != NULL)
            FreeAccessStrategy(scan->rs_strategy);
        scan->rs_strategy = NULL;
    }

    // Configure synchronized scanning
    if (scan->rs_base.rs_parallel != NULL) {
        // Parallel scan: use ParallelTableScanDesc settings
        if (scan->rs_base.rs_parallel->phs_syncscan)
            scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
        else
            scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
    } else if (keep_startblock) {
        // Rescan: keep startblock, update sync setting
        if (allow_sync && synchronize_seqscans)
            scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
        else
            scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
    } else if (allow_sync && synchronize_seqscans) {
        // New scan with sync enabled
        scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
        scan->rs_startblock = ss_get_location(scan->rs_base.rs_rd, scan->rs_nblocks);
    } else {
        // No sync
        scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
        scan->rs_startblock = 0;
    }

    // Initialize scan state
    scan->rs_numblocks = InvalidBlockNumber;
    scan->rs_inited = false;
    scan->rs_ctup.t_data = NULL;
    ItemPointerSetInvalid(&scan->rs_ctup.t_self);
    scan->rs_cbuf = InvalidBuffer;
    scan->rs_cblock = InvalidBlockNumber;
    scan->rs_dir = ForwardScanDirection;
    scan->rs_prefetch_block = InvalidBlockNumber;

    // Copy scan keys
    if (key != NULL && scan->rs_base.rs_nkeys > 0)
        memcpy(scan->rs_base.rs_key, key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData));

    // Update statistics for sequential scans
    if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
        pgstat_count_heap_scan(scan->rs_base.rs_rd);
}
```