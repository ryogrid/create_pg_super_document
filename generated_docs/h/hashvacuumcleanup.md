# hashvacuumcleanup

## Location
[src/backend/access/hash/hash.c:642-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L642-L663)

## Overview
Performs post-VACUUM cleanup operations for hash indexes, primarily updating statistical information for VACUUM displays.

## Definition
```c
IndexBulkDeleteResult *
hashvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
```

## Detailed Description
The hashvacuumcleanup function is called after the bulk deletion phase of a VACUUM operation on a hash index. Its primary responsibility is to finalize the vacuum statistics by adding information that wasn't available during the bulk deletion phase, specifically the total number of pages in the index.

This function is intentionally lightweight compared to hashbulkdelete. If no bulk deletion was performed (indicated by a NULL stats parameter), the function returns NULL to signal that no changes occurred. This covers cases like ANALYZE-only operations where no actual cleanup is needed.

The function updates the num_pages field in the statistics structure with the current number of blocks in the relation, providing a complete picture of the index's state after the vacuum operation.

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing information about the vacuum operation, including the target index relation
- `stats`: IndexBulkDeleteResult structure containing statistics from the bulk deletion phase, or NULL if no bulk deletion was performed

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
- Called from (representative examples):
  - [hashhandler](hashhandler.md) (as part of the hash AM handler functions)

## Notes and Other Information
- Returns NULL if no bulk deletion was performed (stats parameter is NULL), indicating no changes occurred
- This covers the analyze_only case where no actual cleanup is needed
- The function primarily serves to complete the statistical information started by hashbulkdelete
- Much simpler than hashbulkdelete as it only needs to update page count statistics
- Essential for providing accurate information to VACUUM's final report and system catalogs

## Simplified Source

```c
IndexBulkDeleteResult *
hashvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation rel = info->index;
    BlockNumber num_pages;

    // If no bulk deletion was performed, return NULL (no changes)
    // This covers analyze_only operations
    if (stats == NULL)
        return NULL;

    // Update statistics with current page count
    num_pages = RelationGetNumberOfBlocks(rel);
    stats->num_pages = num_pages;

    return stats;
}
```