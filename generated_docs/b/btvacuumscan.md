# btvacuumscan

## Location
[src/backend/access/nbtree/nbtree.c:939-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L939-L1072)

## Overview
Scans the entire B-tree index for VACUUM purposes, identifying deletable tuples, empty pages, and recyclable deleted pages.

## Definition
```c
static void btvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                        IndexBulkDeleteCallback callback, void *callback_state,
                        BTCycleId cycleid)
```

## Detailed Description
This is the core function that performs the actual scanning work for B-tree vacuum operations. It combines multiple tasks in a single pass through the index:

1. **Tuple deletion**: Uses the callback function to identify and delete tuples pointing to dead heap tuples
2. **Page deletion**: Identifies empty pages that can be deleted from the index
3. **Page recycling**: Finds old deleted pages that can be safely recycled and added to the Free Space Map

The function scans all index pages except the metapage in physical order, hoping for read-ahead optimization from the kernel. It handles concurrent page additions by repeatedly checking the relation length and using extension locks to prevent race conditions.

Key implementation details:
- Resets per-scan statistics while preserving per-VACUUM statistics
- Creates a temporary memory context for page deletion operations  
- Initializes pending FSM (Free Space Map) optimization state
- Uses a careful locking protocol to handle concurrent relation extensions
- Updates progress reporting for long-running operations
- Finalizes FSM operations at the end to make deleted pages available for reuse

The function is designed to handle both bulk delete operations (with callback) and cleanup-only operations (callback is NULL).

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum operation details and configuration
- `stats`: IndexBulkDeleteResult structure for accumulating statistics across the scan
- `callback`: IndexBulkDeleteCallback function to determine which tuples should be deleted (NULL for cleanup-only scans)
- `callback_state`: Opaque state data passed to the callback function
- `cycleid`: BTCycleId for coordinating vacuum operations and preventing conflicts

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [_bt_pendingfsm_init](_bt_pendingfsm_init.md)
  - [LockRelationForExtension](../L/LockRelationForExtension.md) / UnlockRelationForExtension  
  - RelationGetNumberOfBlocks
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [btvacuumpage](btvacuumpage.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [_bt_pendingfsm_finalize](_bt_pendingfsm_finalize.md)
  - [IndexFreeSpaceMapVacuum](../I/IndexFreeSpaceMapVacuum.md)
- Types used:
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)
  - IndexBulkDeleteCallback
  - BTCycleId
  - [BTVacState](../B/BTVacState.md)
  - BlockNumber
  - [Relation](../R/Relation.md)
- Constants used:
  - BTREE_METAPAGE
  - ALLOCSET_DEFAULT_SIZES
  - PROGRESS_SCAN_BLOCKS_TOTAL
  - PROGRESS_SCAN_BLOCKS_DONE
  - ExclusiveLock
- Macros used:
  - RELATION_IS_LOCAL
- Called from:
  - [btbulkdelete](btbulkdelete.md)
  - [btvacuumcleanup](btvacuumcleanup.md)

## Notes and Other Information
- This is a static function, only accessible within the nbtree.c file
- Handles both deletion and cleanup-only scans through the same code path
- Uses extension locks to prevent race conditions with concurrent page additions, though this may no longer be necessary with newer page locking mechanisms
- The scan must visit all leaf pages, including those added during the scan, to ensure completeness
- Memory management includes a temporary context for page deletion operations to prevent memory leaks
- Progress reporting is integrated for operations that may take significant time
- FSM (Free Space Map) operations are optimized and batched for efficiency
- The function carefully manages statistics to avoid double-counting in multi-scan VACUUM operations

## Simplified Source

```c
static void btvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                        IndexBulkDeleteCallback callback, void *callback_state,
                        BTCycleId cycleid) {
    Relation rel = info->index;
    BTVacState vstate;
    BlockNumber num_pages, scanblkno;
    bool needLock;

    // Reset per-scan statistics
    stats->num_pages = 0;
    stats->num_index_tuples = 0;
    stats->pages_deleted = 0;
    stats->pages_free = 0;

    // Initialize vacuum state
    vstate.info = info;
    vstate.stats = stats;
    vstate.callback = callback;
    vstate.callback_state = callback_state;
    vstate.cycleid = cycleid;

    // Create temporary memory context for page deletion
    vstate.pagedelcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                 "_bt_pagedel",
                                                 ALLOCSET_DEFAULT_SIZES);

    // Initialize FSM optimization
    vstate.bufsize = 0;
    vstate.maxbufsize = 0;
    vstate.pendingpages = NULL;
    vstate.npendingpages = 0;
    _bt_pendingfsm_init(rel, &vstate, (callback == NULL));

    needLock = !RELATION_IS_LOCAL(rel);
    scanblkno = BTREE_METAPAGE + 1;

    // Main scan loop - process all pages except metapage
    for (;;) {
        // Get current relation length with locking if needed
        if (needLock)
            LockRelationForExtension(rel, ExclusiveLock);
        num_pages = RelationGetNumberOfBlocks(rel);
        if (needLock)
            UnlockRelationForExtension(rel, ExclusiveLock);

        if (info->report_progress)
            pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL, num_pages);

        // Exit if we've scanned all pages
        if (scanblkno >= num_pages)
            break;

        // Process pages in current batch
        for (; scanblkno < num_pages; scanblkno++) {
            btvacuumpage(&vstate, scanblkno);
            if (info->report_progress)
                pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE, scanblkno);
        }
    }

    stats->num_pages = num_pages;

    // Cleanup and finalize FSM operations
    MemoryContextDelete(vstate.pagedelcontext);
    _bt_pendingfsm_finalize(rel, &vstate);
    if (stats->pages_free > 0)
        IndexFreeSpaceMapVacuum(rel);
}
```