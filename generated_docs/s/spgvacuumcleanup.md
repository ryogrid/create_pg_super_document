# spgvacuumcleanup

## Location
[src/backend/access/spgist/spgvacuum.c:947-985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L947-L985)

## Overview
Post-VACUUM cleanup function for SP-GiST indexes that performs additional maintenance when no preceding bulkdelete pass occurred.

## Definition

```c
IndexBulkDeleteResult *
spgvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
```

## Detailed Description
This function performs post-VACUUM cleanup operations for SP-GiST indexes. It handles the cleanup phase that occurs after the main vacuum operation, ensuring proper maintenance of the index structure and providing accurate statistics.

The function operates in two main scenarios:
1. **ANALYZE ONLY mode**: Returns immediately without performing any operations
2. **Normal cleanup mode**: Performs index scanning and cleanup if no preceding bulkdelete pass occurred

When no previous bulkdelete statistics exist, the function initiates a vacuum scan that focuses on redirect/placeholder cleanup and FSM housekeeping rather than deleting live tuples. This ensures the index remains properly maintained.

The function also includes a safeguard against double-counting index tuples that might occur due to concurrent tuple movements during vacuum operations.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum configuration and heap information
- `stats`: IndexBulkDeleteResult structure containing statistics from previous bulkdelete operations (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [spgvacuumscan](spgvacuumscan.md): Performs the actual vacuum scanning operation
  - dummy_callback: Placeholder callback function for vacuum scanning
  - [palloc0](../p/palloc0.md): Memory allocation function
- Called from (representative examples):
  - [spghandler](spghandler.md): SP-GiST access method handler that registers this function

## Notes and Other Information
- The function is a no-op when `info->analyze_only` is true, as cleanup is not needed during ANALYZE-only operations
- Includes logic to prevent over-counting of index tuples by comparing against heap tuple counts when the heap count is known to be accurate
- Part of the SP-GiST access method implementation, specifically handling the vacuum cleanup phase
- Returns a palloc'd IndexBulkDeleteResult structure that must be freed by the caller

## Simplified Source

```c
IndexBulkDeleteResult *
spgvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    spgBulkDeleteState bds;

    // No-op in ANALYZE ONLY mode
    if (info->analyze_only)
        return stats;

    // Perform cleanup scan if no preceding bulkdelete pass
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

        // Initialize state for cleanup scan
        bds.info = info;
        bds.stats = stats;
        bds.callback = dummy_callback;  // Don't delete any tuples
        bds.callback_state = NULL;

        // Perform scan for redirect/placeholder cleanup and FSM maintenance
        spgvacuumscan(&bds);
    }

    // Prevent over-counting due to concurrent tuple movements
    if (!info->estimated_count) {
        if (stats->num_index_tuples > info->num_heap_tuples)
            stats->num_index_tuples = info->num_heap_tuples;
    }

    return stats;
}
```