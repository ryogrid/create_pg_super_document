# find_next_unskippable_block

## Location
[src/backend/access/heap/vacuumlazy.c:1186-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1186-L1284)

## Overview
find_next_unskippable_block uses the visibility map to find the next block in a vacuum scan that cannot be skipped and must be processed.

## Definition

```c
static void
find_next_unskippable_block(LVRelState *vacrel, bool *skipsallvis)
```
## Detailed Description
find_next_unskippable_block implements the core logic for determining which blocks can be safely skipped during vacuum operations based on visibility map information. It evaluates each block according to multiple criteria:

1. **Visibility Map Status**: Blocks that are not all-visible according to the visibility map cannot be skipped as they may contain dead tuples or unfrozen XIDs.

2. **Last Block Rule**: The last block of the relation is always considered unskippable to ensure vacrel->nonempty_pages is set correctly, preventing unnecessary access-exclusive locks during truncation attempts.

3. **Page Skipping Configuration**: Respects the DISABLE_PAGE_SKIPPING option (vacrel->skipwithvm) which forces all blocks to be processed.

4. **Aggressive vs Non-Aggressive Mode**: In aggressive VACUUM mode, only all-frozen blocks can be skipped since all-visible blocks may still contain XIDs < OldestXmin that need freezing. Non-aggressive VACUUMs can skip all-visible blocks but must track this with the skipsallvis flag.

The function maintains visibility map buffer pins and updates vacrel state with the next unskippable block information.

## Parameters / Member Variables
- `*vacrel`: LVRelState containing vacuum state and relation information
- `*skipsallvis`: Output parameter set to true if all-visible (but not all-frozen) blocks are being skipped
## Dependencies
- Functions called/Symbols referenced:
  - [visibilitymap_get_status](../v/visibilitymap_get_status.md) (retrieves visibility map bits for blocks)
  - VISIBILITYMAP_ALL_VISIBLE (visibility map constant)
  - VISIBILITYMAP_ALL_FROZEN (visibility map constant)

- Called from (representative examples):
  - [heap_vac_scan_next_block](../h/heap_vac_scan_next_block.md) (src/backend/access/heap/vacuumlazy.c:1121)

## Notes and Other Information
- Handles race conditions gracefully - it's safe if visibility information becomes stale between check and processing
- The skipsallvis flag ensures relfrozenxid advancement remains safe when all-visible pages are skipped in non-aggressive mode
- Maintains next_unskippable_vmbuffer to preserve visibility map buffer pins across calls
- Critical for vacuum performance optimization while maintaining correctness guarantees about XID/MXID processing
- Updates multiple vacrel fields: next_unskippable_block, next_unskippable_allvis, and next_unskippable_vmbuffer
- Source location: src/backend/access/heap/vacuumlazy.c:1186-1284

## Simplified Source

```c
static void
find_next_unskippable_block(LVRelState *vacrel, bool *skipsallvis)
{
    BlockNumber next_block = vacrel->next_unskippable_block + 1;
    Buffer vmbuffer = vacrel->next_unskippable_vmbuffer;
    bool is_allvis;

    *skipsallvis = false;

    // Scan forward until we find an unskippable block
    for (;;)
    {
        // Check visibility map status for this block
        uint8 mapbits = visibilitymap_get_status(vacrel->rel, next_block, &vmbuffer);
        is_allvis = (mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0;

        // Block is unskippable if not all-visible
        if (!is_allvis)
            break;

        // Always scan the last page to set nonempty_pages correctly
        if (next_block == vacrel->rel_pages - 1)
            break;

        // Skip page skipping if disabled
        if (!vacrel->skipwithvm)
            break;

        // In aggressive mode, can only skip all-frozen blocks
        if ((mapbits & VISIBILITYMAP_ALL_FROZEN) == 0)
        {
            if (vacrel->aggressive)
                break;

            // Non-aggressive can skip all-visible blocks
            *skipsallvis = true;
        }

        next_block++;
    }

    // Update vacrel state with results
    vacrel->next_unskippable_block = next_block;
    vacrel->next_unskippable_allvis = is_allvis;
    vacrel->next_unskippable_vmbuffer = vmbuffer;
}
```