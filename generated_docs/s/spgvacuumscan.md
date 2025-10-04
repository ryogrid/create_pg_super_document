# spgvacuumscan

## Location
[src/backend/access/spgist/spgvacuum.c:804-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L804-L915)

## Overview
Performs the main bulk delete scan for SP-GiST vacuum operations, coordinating page-by-page processing and pending list management.

## Definition

```c
static void
spgvacuumscan(spgBulkDeleteState *bds)
```
## Detailed Description
The `spgvacuumscan` function orchestrates the complete bulk delete scan process for SP-GiST indexes. It implements a two-phase approach:

1. **Main scan phase**: Iterates through all index pages in physical order (excluding metapage), calling `spgvacuumpage` for each page and processing the pending list after each page with `spgprocesspending`.

2. **Finalization phase**: Updates the metapage with cached information, vacuums the Free Space Map (FSM) if pages were deleted, and reports final statistics.

The function uses a dynamic approach to handle concurrent page additions by repeatedly checking the relation length during scanning. It includes provisions for relation extension locking when needed and handles both local and shared relations appropriately.

The scan ensures all leaf pages are visited, which is critical for correctness since deletable tuples might exist on pages added during the scan. It also includes commented-out truncation logic that is disabled due to concurrency concerns.

## Parameters / Member Variables
- `bds`: Bulk delete state containing index relation, statistics, SP-GiST state, pending list, and transaction information

## Dependencies
- Functions called/Symbols referenced:
  - [initSpGistState](../i/initSpGistState.md)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [LockRelationForExtension](../L/LockRelationForExtension.md)/UnlockRelationForExtension
  - RelationGetNumberOfBlocks
  - [spgvacuumpage](spgvacuumpage.md)
  - [spgprocesspending](spgprocesspending.md)
  - [SpGistUpdateMetaPage](../S/SpGistUpdateMetaPage.md)
  - [IndexFreeSpaceMapVacuum](../I/IndexFreeSpaceMapVacuum.md)
  - RELATION_IS_LOCAL
- Called from (representative examples):
  - [spgbulkdelete](spgbulkdelete.md)
  - [spgvacuumcleanup](spgvacuumcleanup.md)

## Notes and Other Information
- Initializes bulk delete statistics and resets counters for multiple scans within a single VACUUM command
- Uses physical page order scanning for better I/O performance with kernel read-ahead
- Handles concurrent relation extension by dynamically checking relation length
- Includes disabled truncation logic due to safety concerns with concurrent operations
- Updates FSM only when empty pages are found to optimize performance
- Critical for maintaining SP-GiST index consistency during vacuum operations
- Similar in concept to btree's btvacuumscan but adapted for SP-GiST's tree structure

## Simplified Source

```c
static void
spgvacuumscan(spgBulkDeleteState *bds)
{
    Relation index = bds->info->index;
    bool needLock;
    BlockNumber num_pages, blkno;

    // Initialize SP-GiST state and bulk delete tracking
    initSpGistState(&bds->spgstate, index);
    bds->pendingList = NULL;
    bds->myXmin = GetActiveSnapshot()->xmin;
    bds->lastFilledBlock = SPGIST_LAST_FIXED_BLKNO;

    // Reset statistics for this scan
    bds->stats->estimated_count = false;
    bds->stats->num_index_tuples = 0;
    bds->stats->pages_deleted = 0;

    // Determine if we need locking for relation extension
    needLock = !RELATION_IS_LOCAL(index);

    // Main scan loop: process all pages except metapage
    blkno = SPGIST_METAPAGE_BLKNO + 1;
    for (;;) {
        // Get current relation length (may grow during scan)
        if (needLock)
            LockRelationForExtension(index, ExclusiveLock);
        num_pages = RelationGetNumberOfBlocks(index);
        if (needLock)
            UnlockRelationForExtension(index, ExclusiveLock);

        // Exit if we've scanned all pages
        if (blkno >= num_pages)
            break;

        // Process pages until we reach current end
        for (; blkno < num_pages; blkno++) {
            // Vacuum individual page
            spgvacuumpage(bds, blkno);

            // Process pending list after each page
            if (bds->pendingList != NULL)
                spgprocesspending(bds);
        }
    }

    // Update metapage with cached lastUsedPages information
    SpGistUpdateMetaPage(index);

    // Vacuum Free Space Map if we deleted any pages
    if (bds->stats->pages_deleted > 0)
        IndexFreeSpaceMapVacuum(index);

    // Note: Index truncation is disabled due to concurrency issues
    // (could invalidate cached page pointers in metapage/other backends)

    // Finalize statistics
    bds->stats->num_pages = num_pages;
    bds->stats->pages_newly_deleted = bds->stats->pages_deleted;
    bds->stats->pages_free = bds->stats->pages_deleted;
}
```