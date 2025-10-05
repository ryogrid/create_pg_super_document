# btvacuumcleanup

## Location
[src/backend/access/nbtree/nbtree.c:851-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L851-L938)

## Overview
Performs post-VACUUM cleanup operations for B-tree indexes, including determining whether a physical scan is needed and maintaining cleanup metadata.

## Definition
```c
IndexBulkDeleteResult *btvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
```

## Detailed Description
This function handles the cleanup phase of VACUUM operations for B-tree indexes. It is called after btbulkdelete (if it was called) to perform final cleanup tasks. The function has two main scenarios:

1. **When btbulkdelete was called**: The function maintains cleanup metadata and validates tuple counts
2. **When btbulkdelete was not called**: The function decides whether a physical index scan is needed based on cleanup requirements

Key operations include:
- Determining if cleanup-only scanning is necessary using _bt_vacuum_needs_cleanup()
- Performing cleanup-only scans when needed (no actual deletions, just maintenance)
- Maintaining the num_delpages counter in the metapage for future cleanup decisions
- Correcting potential tuple count inaccuracies caused by concurrent operations
- Handling posting list tuples which can cause count estimation issues

The function also addresses the challenge of posting list tuples in cleanup-only scans, where the simple count of index tuples per page may underestimate the actual number of TIDs in the index.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum operation details and heap tuple counts
- `stats`: IndexBulkDeleteResult structure with statistics from btbulkdelete (NULL if btbulkdelete wasn't called)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_vacuum_needs_cleanup](_bt_vacuum_needs_cleanup.md)
  - [palloc0](../p/palloc0.md)
  - [btvacuumscan](btvacuumscan.md)
  - [_bt_set_cleanup_info](_bt_set_cleanup_info.md)
- Types used:
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)
  - BlockNumber
- Called from:
  - [bthandler](bthandler.md)

## Notes and Other Information
- Returns NULL for ANALYZE ONLY mode operations
- In cleanup-only scans, the num_index_tuples count is marked as estimated due to posting list complexity
- Maintains num_delpages in the metapage to help future VACUUM operations decide if cleanup is needed
- Includes logic to detect and correct tuple count overestimation caused by concurrent page splits
- Does not use vacuum cycle IDs for cleanup-only operations since no deletions occur
- The function handles the difference between deleted pages and pages actually placed in the Free Space Map (FSM)
- Cleanup-only scans are an optimization to avoid full index scans when no tuples need deletion

## Simplified Source

```c
IndexBulkDeleteResult *btvacuumcleanup(IndexVacuumInfo *info,
                                      IndexBulkDeleteResult *stats) {
    BlockNumber num_delpages;

    // No-op for analyze-only operations
    if (info->analyze_only)
        return stats;

    // If btbulkdelete wasn't called, decide if cleanup scan is needed
    if (stats == NULL) {
        if (!_bt_vacuum_needs_cleanup(info->index))
            return NULL;

        // Perform cleanup-only scan (no deletions)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
        btvacuumscan(info, stats, NULL, NULL, 0);
        stats->estimated_count = true;  // Mark as estimate due to posting lists
    }

    // Update metapage with deleted page count for future cleanup decisions
    num_delpages = stats->pages_deleted - stats->pages_free;
    _bt_set_cleanup_info(info->index, num_delpages);

    // Correct potential overestimation from concurrent page splits
    if (!info->estimated_count) {
        if (stats->num_index_tuples > info->num_heap_tuples)
            stats->num_index_tuples = info->num_heap_tuples;
    }

    return stats;
}
```