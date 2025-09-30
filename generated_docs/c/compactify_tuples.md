# compactify_tuples

## Location
[src/backend/storage/page/bufpage.c:474-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L474-L698)

## Overview
Removes gaps in a page by moving tuples to eliminate fragmentation and reordering them into reverse line pointer order for optimal performance.

## Definition
static void compactify_tuples(itemIdCompact itemidbase, int nitems, Page page, bool presorted)

## Detailed Description
This static function performs tuple compactification to eliminate gaps caused by removed or unused line pointers. It has two optimized code paths depending on whether the input array is presorted. When presorted (tuples in descending order by offset), it uses efficient memmove operations. For non-presorted data, it uses a temporary buffer approach to avoid overwriting tuples during the move operations. The function reorders tuples back into reverse line pointer order, which increases the likelihood of hitting the optimal presorted case in future operations. This is a performance-critical function that includes several optimizations to minimize memory operations.

## Parameters / Member Variables
- itemidbase: Array of itemIdCompact structures representing tuples to be compacted
- nitems: Number of items in the itemidbase array (must be > 0)
- page: The page containing the tuples to be compacted
- presorted: Boolean indicating if itemidbase is sorted in descending order of itemoff

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - memmove
  - memcpy
  - Assert (for debugging)
- Data types used:
  - itemIdCompact
  - PageHeader
  - Offset
  - ItemId
  - PGAlignedBlock
- Called from:
  - [PageRepairFragmentation](../P/PageRepairFragmentation.md) (main page defragmentation function)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md) (bulk tuple deletion operations)

## Notes and Other Information
- This is a static function internal to bufpage.c, not exposed in the public API
- The function includes two distinct algorithms: one for presorted input (using memmove) and one for non-presorted input (using temporary buffer)
- When less than 25% of tuples remain (>75% pruned), it uses a tuple-by-tuple copy approach for better efficiency
- The function includes extensive assertions in debug builds to verify the presorted parameter
- Performance-optimized to minimize memory operations and take advantage of common access patterns
- Updates the page header's pd_upper field to reflect the new free space boundary
- The function is located in src/backend/storage/page/bufpage.c:474-698

## Simplified Source

```c
static void
compactify_tuples(itemIdCompact itemidbase, int nitems, Page page, bool presorted)
{
    PageHeader phdr = (PageHeader) page;
    Offset upper;
    Offset copy_tail, copy_head;
    itemIdCompact itemidptr;
    int i;

    Assert(nitems > 0);

    if (presorted)
    {
        // Optimized path: tuples already in reverse order by offset
        // Can use efficient memmove operations

        upper = phdr->pd_special;
        i = 0;

        // Skip tuples already at the end in correct position
        do
        {
            itemidptr = &itemidbase[i];
            if (upper != itemidptr->itemoff + itemidptr->alignedlen)
                break;
            upper -= itemidptr->alignedlen;
            i++;
        } while (i < nitems);

        // Move remaining tuples with minimal memmove calls
        copy_tail = copy_head = itemidptr->itemoff + itemidptr->alignedlen;
        for (; i < nitems; i++)
        {
            ItemId lp;
            itemidptr = &itemidbase[i];
            lp = PageGetItemId(page, itemidptr->offsetindex + 1);

            // Move when gap detected
            if (copy_head != itemidptr->itemoff + itemidptr->alignedlen)
            {
                memmove((char *) page + upper, page + copy_head, copy_tail - copy_head);
                copy_tail = itemidptr->itemoff + itemidptr->alignedlen;
            }

            upper -= itemidptr->alignedlen;
            copy_head = itemidptr->itemoff;
            lp->lp_off = upper;  // Update line pointer
        }

        // Move final chunk
        memmove((char *) page + upper, page + copy_head, copy_tail - copy_head);
    }
    else
    {
        // Non-presorted path: use temporary buffer to avoid overwrites
        PGAlignedBlock scratch;
        char *scratchptr = scratch.data;

        upper = phdr->pd_special;

        // Choose copy strategy based on pruning ratio
        if (nitems < PageGetMaxOffsetNumber(page) / 4)
        {
            // Heavy pruning: copy tuple by tuple
            for (i = 0; i < nitems; i++)
            {
                itemidptr = &itemidbase[i];
                memcpy(scratchptr + itemidptr->itemoff, page + itemidptr->itemoff,
                       itemidptr->alignedlen);
            }
            i = 0;
            itemidptr = &itemidbase[0];
            upper = phdr->pd_special;
        }
        else
        {
            // Light pruning: bulk copy with skip optimization
            i = 0;
            do
            {
                itemidptr = &itemidbase[i];
                if (upper != itemidptr->itemoff + itemidptr->alignedlen)
                    break;
                upper -= itemidptr->alignedlen;
                i++;
            } while (i < nitems);

            // Copy all movable tuples to scratch buffer
            memcpy(scratchptr + phdr->pd_upper, page + phdr->pd_upper,
                   upper - phdr->pd_upper);
        }

        // Move tuples from scratch buffer back to correct positions
        copy_tail = copy_head = itemidptr->itemoff + itemidptr->alignedlen;
        for (; i < nitems; i++)
        {
            ItemId lp;
            itemidptr = &itemidbase[i];
            lp = PageGetItemId(page, itemidptr->offsetindex + 1);

            // Copy when gap detected
            if (copy_head != itemidptr->itemoff + itemidptr->alignedlen)
            {
                memcpy((char *) page + upper, scratchptr + copy_head,
                       copy_tail - copy_head);
                copy_tail = itemidptr->itemoff + itemidptr->alignedlen;
            }

            upper -= itemidptr->alignedlen;
            copy_head = itemidptr->itemoff;
            lp->lp_off = upper;  // Update line pointer
        }

        // Copy final chunk from scratch buffer
        memcpy((char *) page + upper, scratchptr + copy_head, copy_tail - copy_head);
    }

    // Update page header with new upper boundary
    phdr->pd_upper = upper;
}
```