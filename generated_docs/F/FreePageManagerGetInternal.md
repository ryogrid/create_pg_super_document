# FreePageManagerGetInternal

## Location
[src/backend/utils/mmgr/freepage.c:1319-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1319-L1475)

## Overview
The core allocation function that finds and allocates a contiguous run of pages from the Free Page Manager using a best-fit strategy across multiple freelists.

## Definition
```c
static bool FreePageManagerGetInternal(FreePageManager *fpm, Size npages, Size *first_page)
```

## Detailed Description
This function implements the main page allocation logic for the Free Page Manager. It searches through freelists using a best-fit policy, starting with the appropriately-sized list and moving to larger lists as needed. For fixed-size lists, it takes the first available span, but for the oversized list (last freelist), it searches for the smallest span that satisfies the request. After finding a suitable span, it removes it from the freelist, updates the B-tree structure if necessary, handles partial allocation by splitting spans, and maintains the contiguous_pages tracking. The function handles both the initial singleton span case and the full B-tree managed case.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance
- `npages`: Number of contiguous pages to allocate
- `first_page`: Output parameter receiving the starting page number of the allocated span

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_pointer_to_page  
  - relptr_access
  - relptr_copy
  - relptr_is_null
  - [FreePagePushSpanLeader](FreePagePushSpanLeader.md)
  - [FreePageBtreeSearch](FreePageBtreeSearch.md)
  - [FreePageBtreeRemove](FreePageBtreeRemove.md)
  - [FreePageBtreeAdjustAncestorKeys](FreePageBtreeAdjustAncestorKeys.md)
- Called from (representative examples):
  - [FreePageManagerGet](FreePageManagerGet.md)
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md) (internal use for span consolidation)

## Notes and Other Information
- Uses best-fit allocation strategy which may cause fragmentation but is suitable for PostgreSQL's typical allocation patterns
- Handles both singleton span mode (before B-tree initialization) and full B-tree mode
- Updates contiguous_pages tracking when removing spans that might affect the largest available span
- Splits spans when allocated size is smaller than found span
- Returns false if no suitable span is available
- Maintains freelist integrity by properly unlinking spans and updating prev/next pointers
- Critical function for PostgreSQL's memory management subsystem performance

## Simplified Source

```c
static bool
FreePageManagerGetInternal(FreePageManager *fpm, Size npages, Size *first_page)
{
    char *base = fpm_segment_base(fpm);
    FreePageSpanLeader *victim = NULL;
    FreePageSpanLeader *prev, *next;
    FreePageBtreeSearchResult result;
    Size victim_page = 0;
    Size f;

    // Search for a free span using best-fit policy
    // Start from appropriate freelist size and search upward
    for (f = Min(npages, FPM_NUM_FREELISTS) - 1; f < FPM_NUM_FREELISTS; ++f)
    {
        // Skip empty freelists
        if (relptr_is_null(fpm->freelist[f]))
            continue;

        // For fixed-size lists, take first available span
        // For oversized list, search for best fit
        if (f < FPM_NUM_FREELISTS - 1)
        {
            victim = relptr_access(base, fpm->freelist[f]);
        }
        else
        {
            // Search oversized list for smallest suitable span
            FreePageSpanLeader *candidate = relptr_access(base, fpm->freelist[f]);
            do
            {
                if (candidate->npages >= npages &&
                    (victim == NULL || victim->npages > candidate->npages))
                {
                    victim = candidate;
                    if (victim->npages == npages)
                        break;  // Perfect fit found
                }
                candidate = relptr_access(base, candidate->next);
            } while (candidate != NULL);
        }
        break;
    }

    // Return failure if no suitable span found
    if (victim == NULL)
        return false;

    // Remove span from freelist
    Assert(victim->magic == FREE_PAGE_SPAN_LEADER_MAGIC);
    prev = relptr_access(base, victim->prev);
    next = relptr_access(base, victim->next);

    // Update linked list pointers
    if (prev != NULL)
        relptr_copy(prev->next, victim->next);
    else
        relptr_copy(fpm->freelist[f], victim->next);
    if (next != NULL)
        relptr_copy(next->prev, victim->prev);

    victim_page = fpm_pointer_to_page(base, victim);

    // Update contiguous_pages tracking if necessary
    if ((f == FPM_NUM_FREELISTS - 1 && victim->npages == fpm->contiguous_pages) ||
        (f + 1 == fpm->contiguous_pages && relptr_is_null(fpm->freelist[f])))
    {
        fpm->contiguous_pages_dirty = true;
    }

    // Handle allocation based on btree state
    if (relptr_is_null(fpm->btree_root))
    {
        // Singleton mode - update single span
        Assert(victim_page == fpm->singleton_first_page);
        Assert(victim->npages == fpm->singleton_npages);
        Assert(victim->npages >= npages);

        fpm->singleton_first_page += npages;
        fpm->singleton_npages -= npages;

        // Re-add remaining pages if any
        if (fpm->singleton_npages > 0)
            FreePagePushSpanLeader(fpm, fpm->singleton_first_page,
                                 fpm->singleton_npages);
    }
    else
    {
        // Btree mode - update btree and handle partial allocation
        FreePageBtreeSearch(fpm, victim_page, &result);
        Assert(result.found);

        if (victim->npages == npages)
        {
            // Exact fit - remove from btree
            FreePageBtreeRemove(fpm, result.page, result.index);
        }
        else
        {
            // Partial allocation - update btree and re-add remainder
            FreePageBtreeLeafKey *key = &result.page->u.leaf_key[result.index];
            Assert(key->npages == victim->npages);

            key->first_page += npages;
            key->npages -= npages;

            // Update ancestor keys if first entry changed
            if (result.index == 0)
                FreePageBtreeAdjustAncestorKeys(fpm, result.page);

            // Put unallocated pages back on freelist
            FreePagePushSpanLeader(fpm, victim_page + npages,
                                 victim->npages - npages);
        }
    }

    // Return allocated page number
    *first_page = fpm_pointer_to_page(base, victim);
    return true;
}
```