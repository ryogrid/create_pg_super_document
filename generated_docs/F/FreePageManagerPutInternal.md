# FreePageManagerPutInternal

## Location
[src/backend/utils/mmgr/freepage.c:1476-1842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1476-L1842)

## Overview
The core deallocation function that returns a range of pages to the Free Page Manager, handling consolidation with adjacent spans and B-tree management including splits and reorganization.

## Definition
```c
static Size FreePageManagerPutInternal(FreePageManager *fpm, Size first_page, Size npages, bool soft)
```

## Detailed Description
This complex function handles returning freed pages to the Free Page Manager with sophisticated consolidation and B-tree management. It operates in several modes: singleton mode (before B-tree initialization) where it manages a single free span and handles consolidation by extending existing spans or initializing the B-tree when non-contiguous. In B-tree mode, it searches for insertion points, consolidates with adjacent entries (both preceding and following), and performs B-tree operations including splits when necessary. The function can operate in 'soft' mode where it avoids allocating new B-tree pages, useful for cleanup operations. It maintains freelist integrity throughout all operations and returns the size of the final consolidated span.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance
- `first_page`: Starting page number of the span being returned
- `npages`: Number of contiguous pages being returned
- `soft`: If true, avoid operations that would require allocating new B-tree pages

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_page_to_pointer
  - [FreePagePushSpanLeader](FreePagePushSpanLeader.md)
  - [FreePagePopSpanLeader](FreePagePopSpanLeader.md)
  - [FreePageBtreeGetRecycled](FreePageBtreeGetRecycled.md)
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)
  - [FreePageBtreeSearch](FreePageBtreeSearch.md)
  - [FreePageBtreeFindRightSibling](FreePageBtreeFindRightSibling.md)
  - [FreePageBtreeRemove](FreePageBtreeRemove.md)
  - [FreePageBtreeAdjustAncestorKeys](FreePageBtreeAdjustAncestorKeys.md)
  - [FreePageBtreeRecycle](FreePageBtreeRecycle.md)
  - [FreePageBtreeSplitPage](FreePageBtreeSplitPage.md)
  - [FreePageBtreeInsertLeaf](FreePageBtreeInsertLeaf.md)
  - [FreePageBtreeInsertInternal](FreePageBtreeInsertInternal.md)
  - [FreePageBtreeSearchLeaf](FreePageBtreeSearchLeaf.md)
  - [FreePageBtreeSearchInternal](FreePageBtreeSearchInternal.md)
  - [FreePageBtreeFirstKey](FreePageBtreeFirstKey.md)
  - relptr_store
  - relptr_access
- Called from (representative examples):
  - [FreePageManagerPut](FreePageManagerPut.md)
  - [FreePageBtreeCleanup](FreePageBtreeCleanup.md)

## Notes and Other Information
- Returns 0 if soft flag prevented insertion, otherwise returns size of consolidated span
- Handles both singleton mode (before B-tree initialization) and full B-tree mode
- Performs intelligent consolidation with adjacent free spans to reduce fragmentation
- Manages B-tree splits including root splits that increase tree depth
- Uses recycled B-tree pages when available to minimize allocation overhead
- Critical function for PostgreSQL's memory deallocation and defragmentation
- Can trigger complex multi-level B-tree restructuring operations
- Maintains consistency between B-tree structure and freelists throughout all operations

## Simplified Source

```c
// Simplified version of FreePageManagerPutInternal
static Size FreePageManagerPutInternal(FreePageManager *fpm, Size first_page, Size npages, bool soft) {
    char *base = fpm_segment_base(fpm);
    FreePageBtreeSearchResult result;

    Assert(npages > 0);

    // Handle singleton mode (before btree is initialized)
    if (fpm->btree_depth == 0) {
        if (fpm->singleton_npages == 0) {
            // No existing span - store this one
            fpm->singleton_first_page = first_page;
            fpm->singleton_npages = npages;
            FreePagePushSpanLeader(fpm, first_page, npages);
            return npages;
        }
        else if (can_consolidate_with_singleton(fpm, first_page, npages)) {
            // Consolidate with existing singleton span
            consolidate_singleton_span(fpm, first_page, npages);
            return fpm->singleton_npages;
        }
        else {
            // Not contiguous - need to initialize btree
            if (soft) return 0;  // Don't allocate if soft mode

            initialize_btree_from_singleton(fpm, first_page, npages);
            // Fall through to btree insertion
        }
    }

    // Search btree for insertion point
    FreePageBtreeSearch(fpm, first_page, &result);

    // Find adjacent entries for potential consolidation
    FreePageBtreeLeafKey *prev_key = get_previous_key(&result);
    FreePageBtreeLeafKey *next_key = get_next_key(&result);

    // Try to consolidate with previous entry
    if (prev_key && spans_are_adjacent(prev_key, first_page)) {
        Size consolidated_size = consolidate_with_previous(fpm, prev_key, first_page, npages);

        // Also try to consolidate with next entry
        if (next_key && spans_are_adjacent_after_consolidation(prev_key, next_key)) {
            consolidated_size = consolidate_with_next_as_well(fpm, prev_key, next_key);
            FreePageBtreeRemove(fpm, next_key);  // Remove the absorbed entry
        }

        update_freelists(fpm, prev_key->first_page, consolidated_size);
        return consolidated_size;
    }

    // Try to consolidate with next entry only
    if (next_key && spans_are_adjacent_before(first_page, npages, next_key)) {
        Size consolidated_size = consolidate_with_next(fpm, first_page, npages, next_key);
        update_freelists(fpm, first_page, consolidated_size);

        // Update key in place and adjust ancestors if needed
        next_key->first_page = first_page;
        next_key->npages = consolidated_size;
        if (is_first_key_on_page(&result)) {
            FreePageBtreeAdjustAncestorKeys(fpm, result.page);
        }

        return consolidated_size;
    }

    // No consolidation possible - need to insert new entry
    if (btree_needs_split(&result)) {
        if (soft) return 0;  // Don't split if soft mode

        // Ensure we have enough recycled pages for splitting
        ensure_split_pages_available(fpm, &result);

        // Perform the split operation
        perform_btree_split_and_insert(fpm, &result, first_page, npages);
    }
    else {
        // Simple insertion - page has space
        FreePageBtreeInsertLeaf(result.page, result.index, first_page, npages);

        // Adjust ancestor keys if this is the new first key
        if (result.index == 0) {
            FreePageBtreeAdjustAncestorKeys(fpm, result.page);
        }
    }

    // Add to freelist and return
    FreePagePushSpanLeader(fpm, first_page, npages);
    return npages;
}
```

Key simplifications made:
- Abstracted complex consolidation logic into helper function calls
- Simplified singleton mode handling by grouping similar cases
- Reduced complex btree split logic to high-level operations
- Removed detailed error handling and edge case management
- Consolidated similar consolidation patterns
- Abstracted low-level pointer arithmetic and memory operations
- Simplified btree navigation and key management logic