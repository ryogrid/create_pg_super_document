# mask_page_hint_bits

## Location
[src/backend/access/common/bufmask.c:46-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/bufmask.c#L46-L70)

## Overview
Masks hint bits in page headers that can be set without emitting WAL records, enabling consistent page comparisons during WAL consistency checks.

## Definition
```c
void mask_page_hint_bits(Page page)
```

## Detailed Description
This function masks various hint bits in a page header that can change without generating WAL records. Hint bits are optimization flags that improve performance but are not critical for correctness. Since these bits can be set asynchronously and don't affect the logical content of the page, they must be masked out during consistency checks to avoid false mismatches.

The function clears several types of hint information:
1. The prune_xid field (set to MASK_MARKER)
2. Page fullness flags (PD_PAGE_FULL and PD_HAS_FREE_LINES)
3. All-visible flag (PD_ALL_VISIBLE)

These modifications ensure that pages can be compared based on their actual data content rather than transient hint information.

## Parameters / Member Variables
- `page`: A pointer to the page whose hint bits should be masked

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast for page header access)
  - MASK_MARKER (constant used as mask value)
  - [PageClearFull](../P/PageClearFull.md) (macro to clear page full flag)
  - [PageClearHasFreeLinePointers](../P/PageClearHasFreeLinePointers.md) (macro to clear free line pointers flag)  
  - [PageClearAllVisible](../P/PageClearAllVisible.md) (macro to clear all-visible flag)
- Called from (representative examples):
  - [brin_mask](../b/brin_mask.md) (BRIN index masking)
  - [gin_mask](../g/gin_mask.md) (GIN index masking)
  - [gist_mask](../g/gist_mask.md) (GiST index masking)
  - [hash_mask](../h/hash_mask.md) (hash index masking)
  - [heap_mask](../h/heap_mask.md) (heap page masking)
  - [btree_mask](../b/btree_mask.md) (B-tree index masking)
  - [spg_mask](../s/spg_mask.md) (SP-GiST index masking)

## Notes and Other Information
- Hint bits are performance optimizations that can be set independently of WAL generation
- The pd_prune_xid field tracks the oldest XID that might have dead tuples on the page
- Page fullness flags help the system make better decisions about tuple placement
- The all-visible flag indicates whether all tuples on the page are visible to all transactions
- This masking is essential for WAL consistency verification since hint bits can differ between original and replayed pages
- The function modifies the page in-place and does not return any value