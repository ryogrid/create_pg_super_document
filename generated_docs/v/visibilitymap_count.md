# visibilitymap_count

## Location
[src/backend/access/heap/visibilitymap.c:384-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/visibilitymap.c#L384-L437)

## Overview
Counts the number of all-visible and optionally all-frozen pages in a relation by scanning the visibility map and counting set bits.

## Definition

```c
void
visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen)
```
## Detailed Description
This function iterates through all blocks of a relation's visibility map to count pages marked as all-visible and all-frozen. It reads each map block sequentially using vm_readbuf() and uses pg_popcount_masked() to efficiently count bits set in the visibility map pages. The function is designed to provide approximate counts and ignores potential race conditions from concurrent table extensions, as new pages won't be marked visible/frozen immediately. The function doesn't lock map pages since the results would be immediately stale in concurrent scenarios.

## Parameters / Member Variables
- `rel`: The relation whose visibility map should be scanned
- `*all_visible`: Output parameter to store the count of all-visible pages (required)
- `*all_frozen`: Output parameter to store the count of all-frozen pages (optional, can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [vm_readbuf](vm_readbuf.md)
  - [PageGetContents](../P/PageGetContents.md)
  - [pg_popcount_masked](../p/pg_popcount_masked.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - VISIBLE_MASK8
  - FROZEN_MASK8
  - MAPSIZE
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [index_update_stats](../i/index_update_stats.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- The function assumes extra bytes in the last page are zeroed and includes them in the count
- Race conditions with concurrent table extensions are explicitly ignored as they don't affect accuracy significantly
- Uses bitwise masking with VISIBLE_MASK8 and FROZEN_MASK8 to count specific bit patterns
- Part of PostgreSQL's visibility map system for tracking page-level visibility information

## Simplified Source

```c
void visibilitymap_count(Relation rel, BlockNumber *all_visible, BlockNumber *all_frozen) {
    BlockNumber nvisible = 0;
    BlockNumber nfrozen = 0;

    // all_visible parameter is required
    Assert(all_visible);

    // Iterate through all visibility map blocks
    for (BlockNumber mapBlock = 0;; mapBlock++) {
        // Read the visibility map buffer
        Buffer mapBuffer = vm_readbuf(rel, mapBlock, false);
        if (!BufferIsValid(mapBuffer)) {
            break;  // End of map reached
        }

        // Get the map data (no locking for approximate count)
        uint64 *map = (uint64 *) PageGetContents(BufferGetPage(mapBuffer));

        // Count visible and frozen bits
        nvisible += pg_popcount_masked((const char *) map, MAPSIZE, VISIBLE_MASK8);
        if (all_frozen) {
            nfrozen += pg_popcount_masked((const char *) map, MAPSIZE, FROZEN_MASK8);
        }

        ReleaseBuffer(mapBuffer);
    }

    // Return the counts
    *all_visible = nvisible;
    if (all_frozen) {
        *all_frozen = nfrozen;
    }
}
```