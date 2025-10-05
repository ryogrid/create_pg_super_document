# lazy_vacuum

## Location
[src/backend/access/heap/vacuumlazy.c:1865-1989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1865-L1989)

## Overview
Main coordinator function for index vacuuming and heap vacuuming that removes collected dead items from indexes and marks them as unused in heap pages.

## Definition
```c
static void lazy_vacuum(LVRelState *vacrel)
```

## Detailed Description
lazy_vacuum serves as the main entry point for the index and heap vacuuming phases of the VACUUM operation. It orchestrates the removal of LP_DEAD items that have been collected during the heap scan phase. The function implements intelligent bypass optimization that can skip index vacuuming when there are very few dead items, avoiding performance discontinuities for tables with mostly HOT updates. It coordinates with lazy_vacuum_all_indexes to perform index cleaning and lazy_vacuum_heap_rel to mark items as unused in heap pages. The function also handles failsafe scenarios where index vacuuming cannot complete due to very old transaction IDs, falling back to safe operation modes.

## Parameters / Member Variables
- `vacrel`: LVRelState containing the complete VACUUM operation state, dead items collection, and configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - [dead_items_reset](../d/dead_items_reset.md)
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md)
  - [lazy_vacuum_all_indexes](lazy_vacuum_all_indexes.md)
  - [lazy_vacuum_heap_rel](lazy_vacuum_heap_rel.md)
  - BYPASS_THRESHOLD_PAGES
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md) (multiple call sites)

## Notes and Other Information
- Only called for relations with indexes (nindexes > 0)
- Implements bypass optimization based on BYPASS_THRESHOLD_PAGES percentage threshold
- Bypass optimization prevents performance discontinuities in workloads with mostly HOT updates
- Memory usage check limits bypass optimization to cases under 32MB of dead items storage
- Handles failsafe mode when VacuumFailsafeActive prevents full index scans
- Always calls dead_items_reset to free memory after processing
- The bypass optimization improves performance consistency for tables with sporadic non-HOT updates
- Coordinates the two-phase approach of index vacuuming followed by heap vacuuming

## Simplified Source

```c
static void
lazy_vacuum(LVRelState *vacrel)
{
    bool bypass = false;

    Assert(vacrel->nindexes > 0);
    Assert(vacrel->lpdead_item_pages > 0);

    // Skip vacuuming if disabled
    if (!vacrel->do_index_vacuuming)
    {
        dead_items_reset(vacrel);
        return;
    }

    // Consider bypassing index vacuuming for optimization
    if (vacrel->consider_bypass_optimization && vacrel->rel_pages > 0)
    {
        BlockNumber threshold = (double) vacrel->rel_pages * BYPASS_THRESHOLD_PAGES;

        // Bypass if very few dead items and memory usage is low
        bypass = (vacrel->lpdead_item_pages < threshold &&
                 (TidStoreMemoryUsage(vacrel->dead_items) < (32L * 1024L * 1024L)));
    }

    if (bypass)
    {
        // Bypass index vacuuming but do cleanup
        vacrel->do_index_vacuuming = false;
    }
    else if (lazy_vacuum_all_indexes(vacrel))
    {
        // Successfully completed index vacuuming, now vacuum heap
        lazy_vacuum_heap_rel(vacrel);
    }
    else
    {
        // Failsafe case - couldn't complete due to wraparound risk
        Assert(VacuumFailsafeActive);
    }

    // Clean up dead items memory
    dead_items_reset(vacrel);
}
```