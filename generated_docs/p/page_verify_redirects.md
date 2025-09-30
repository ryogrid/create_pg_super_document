# page_verify_redirects

## Location
[src/backend/access/heap/pruneheap.c:1737-1784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1737-L1784)

## Overview
A debugging function that verifies the integrity of LP_REDIRECT line pointer items on a heap page, ensuring that all redirect items point to valid heap-only tuples.

## Definition

```c
static void
page_verify_redirects(Page page)
```
## Detailed Description
This function is compiled only when assertions are enabled (USE_ASSERT_CHECKING) and serves as a verification tool for HOT (Heap Only Tuples) pruning operations. It iterates through all line pointers on a page and validates that any LP_REDIRECT items correctly point to valid heap-only tuples.

The function performs comprehensive validation to catch bugs in HOT pruning logic where redirect items might incorrectly point to removed tuples. This type of corruption could otherwise go undetected until much later in execution, making debugging difficult. The verification is particularly important because it's not trivial to reliably check redirect validity during the actual pruning operations in heap_prune_chain() and heap_page_prune_execute().

## Parameters / Member Variables
- : The heap page to verify redirect items on

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsRedirected
  - ItemIdGetRedirect
  - ItemIdIsUsed
  - ItemIdIsNormal
  - ItemIdHasStorage
  - [PageGetItem](../P/PageGetItem.md)
  - HeapTupleHeaderIsHeapOnly
  - FirstOffsetNumber
  - OffsetNumberNext
- Called from (representative examples):
  - [heap_page_prune_execute](../h/heap_page_prune_execute.md)

## Notes and Other Information
- This function only executes when PostgreSQL is built with assertion checking enabled
- It's specifically designed to catch HOT pruning bugs where redirect items become orphaned
- The verification happens after pruning operations to ensure page consistency
- All redirect items must point to heap-only tuples that are properly marked and have storage
- This is a critical debugging tool for maintaining heap page integrity during tuple pruning operations

## Simplified Source

```c
static void page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
    OffsetNumber offnum, maxoff;

    // Iterate through all line pointers on the page
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
    {
        ItemId itemid = PageGetItemId(page, offnum);

        // Skip non-redirect items
        if (!ItemIdIsRedirected(itemid))
            continue;

        // Get the target of the redirect
        OffsetNumber targoff = ItemIdGetRedirect(itemid);
        ItemId targitem = PageGetItemId(page, targoff);

        // Verify target item is valid and points to a heap-only tuple
        Assert(ItemIdIsUsed(targitem));
        Assert(ItemIdIsNormal(targitem));
        Assert(ItemIdHasStorage(targitem));

        HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, targitem);
        Assert(HeapTupleHeaderIsHeapOnly(htup));
    }
#endif
}
```