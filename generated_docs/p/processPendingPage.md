# processPendingPage

## Location
[src/backend/access/gin/ginfast.c:709-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L709-L779)

## Overview
Collects data from a pending-list page in preparation for insertion into the main GIN index by processing all tuples and accumulating their keys.

## Definition
```c
static void processPendingPage(BuildAccumulator *accum, KeyArray *ka, Page page, OffsetNumber startoff)
```

## Detailed Description
This function processes all tuples starting from a specified offset on a pending-list page, extracting their keys and categories for insertion into the main GIN index. It groups keys by heap tuple ID and attribute number, calling ginInsertBAEntries whenever it encounters a boundary (change in heap TID or attribute). The function efficiently handles the transition from the fast insertion pending list to the main index structure by collecting and organizing the data appropriately.

## Parameters / Member Variables
- `accum`: Pointer to BuildAccumulator structure for collecting index entries
- `ka`: Pointer to KeyArray workspace for temporarily storing keys (reset on each call)
- `page`: The pending-list page to process
- `startoff`: The offset number to start processing from

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (get maximum offset on page)
  - [PageGetItem](../P/PageGetItem.md), PageGetItemId (page item access functions)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md) (extract attribute number from tuple)
  - [gintuple_get_key](../g/gintuple_get_key.md) (extract key and category from tuple)
  - [ginInsertBAEntries](../g/ginInsertBAEntries.md) (insert accumulated entries into build accumulator)
  - [addDatum](../a/addDatum.md) (add datum to KeyArray)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md), ItemPointerIsValid, ItemPointerEquals (heap pointer utilities)
  - OffsetNumberNext (offset number iteration)
- Called from (representative examples):
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (at src/backend/access/gin/ginfast.c:893)
  - [ginInsertCleanup](../g/ginInsertCleanup.md) (at src/backend/access/gin/ginfast.c:954)

## Notes and Other Information
- This is a static function, accessible only within the ginfast.c file
- The KeyArray (ka) serves as workspace and doesn't carry state across calls
- Groups keys by heap tuple ID and attribute number for efficient batch insertion
- Processes tuples sequentially from startoff to the maximum offset on the page
- Calls ginInsertBAEntries at boundaries to handle batched insertions
- Ensures all remaining keys are dumped out after processing all tuples
- Part of the GIN index pending list cleanup mechanism
- Handles the transition from fast insertion to main index structure

## Simplified Source

```c
// Simplified version of processPendingPage
static void processPendingPage(BuildAccumulator *accum, KeyArray *ka,
                              Page page, OffsetNumber startoff)
{
    ItemPointerData heapptr;
    OffsetNumber i, maxoff, attrnum;

    // Reset workspace and initialize variables
    ka->nvalues = 0;
    maxoff = PageGetMaxOffsetNumber(page);
    ItemPointerSetInvalid(&heapptr);
    attrnum = 0;

    // Process each tuple on the page
    for (i = startoff; i <= maxoff; i = OffsetNumberNext(i)) {
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
        OffsetNumber curattnum;
        Datum curkey;
        GinNullCategory curcategory;

        // Extract attribute number from tuple
        curattnum = gintuple_get_attrnum(accum->ginstate, itup);

        // Check for heap TID or attribute change
        if (!ItemPointerIsValid(&heapptr)) {
            // First tuple - initialize
            heapptr = itup->t_tid;
            attrnum = curattnum;
        } else if (!(ItemPointerEquals(&heapptr, &itup->t_tid) &&
                    curattnum == attrnum)) {
            // Boundary detected - insert accumulated entries
            ginInsertBAEntries(accum, &heapptr, attrnum,
                              ka->keys, ka->categories, ka->nvalues);
            ka->nvalues = 0;
            heapptr = itup->t_tid;
            attrnum = curattnum;
        }

        // Extract key and add to workspace
        curkey = gintuple_get_key(accum->ginstate, itup, &curcategory);
        addDatum(ka, curkey, curcategory);
    }

    // Insert remaining keys
    ginInsertBAEntries(accum, &heapptr, attrnum,
                      ka->keys, ka->categories, ka->nvalues);
}
```