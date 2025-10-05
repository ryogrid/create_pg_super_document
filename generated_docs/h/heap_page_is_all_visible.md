# heap_page_is_all_visible

## Location
[src/backend/access/heap/vacuumlazy.c:2955-3070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2955-L3070)

## Overview
`heap_page_is_all_visible` determines whether every tuple in a heap page is visible to all current and future transactions, also identifying the visibility cutoff transaction ID and freeze status.

## Definition
```c
static bool heap_page_is_all_visible(LVRelState *vacrel, Buffer buf, TransactionId *visibility_cutoff_xid, bool *all_frozen)
```

## Detailed Description
This function is a specialized visibility checker that examines every tuple on a heap page to determine if the entire page can be marked as all-visible in the visibility map. It performs a comprehensive scan of all line pointers and tuples, checking their visibility status using HeapTupleSatisfiesVacuum. The function also tracks the highest xmin value among visible tuples (visibility_cutoff_xid) and determines if all tuples are frozen. This is essentially a stripped-down version of lazy_scan_prune, optimized specifically for visibility checking without performing actual cleanup operations.

The function handles various tuple states including live, dead, recently dead, and in-progress transactions, only considering a page all-visible if every tuple is definitively visible to all transactions.

## Parameters / Member Variables
- `vacrel`: Pointer to LVRelState structure containing vacuum operation state and cutoff information
- `buf`: Buffer containing the heap page to examine
- `visibility_cutoff_xid`: Output parameter set to the highest xmin among visible tuples
- `all_frozen`: Output parameter indicating whether all tuples on the page are frozen

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsRedirected
  - ItemIdIsDead
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderGetXmin
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - TransactionIdIsNormal
  - [heap_tuple_needs_eventual_freeze](heap_tuple_needs_eventual_freeze.md)
- Called from (representative examples):
  - [lazy_scan_prune](../l/lazy_scan_prune.md)
  - [lazy_vacuum_heap_page](../l/lazy_vacuum_heap_page.md)

## Notes and Other Information
- This is a static function, only accessible within vacuumlazy.c
- The function is designed to stay in sync with lazy_scan_prune and should be updated when that function changes
- Dead line pointers prevent a page from being all-visible since they may have index pointers
- The function sets and clears `vacrel->offnum` for error reporting purposes
- Only committed, old enough transactions with normal XIDs contribute to visibility_cutoff_xid
- A page can be all-visible but not all-frozen if it contains unfrozen but visible tuples

## Simplified Source

```c
static bool
heap_page_is_all_visible(LVRelState *vacrel, Buffer buf,
                         TransactionId *visibility_cutoff_xid,
                         bool *all_frozen)
{
    Page page = BufferGetPage(buf);
    BlockNumber blockno = BufferGetBlockNumber(buf);
    OffsetNumber offnum, maxoff;
    bool all_visible = true;

    *visibility_cutoff_xid = InvalidTransactionId;
    *all_frozen = true;

    // Scan all line pointers on the page
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff && all_visible;
         offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        HeapTupleData tuple;

        vacrel->offnum = offnum;
        itemid = PageGetItemId(page, offnum);

        // Skip unused or redirect line pointers
        if (!ItemIdIsUsed(itemid) || ItemIdIsRedirected(itemid))
            continue;

        ItemPointerSet(&(tuple.t_self), blockno, offnum);

        // Dead line pointers make page not all-visible
        if (ItemIdIsDead(itemid)) {
            all_visible = false;
            *all_frozen = false;
            break;
        }

        // Set up tuple for visibility check
        tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationGetRelid(vacrel->rel);

        switch (HeapTupleSatisfiesVacuum(&tuple, vacrel->cutoffs.OldestXmin, buf)) {
            case HEAPTUPLE_LIVE:
                {
                    TransactionId xmin;

                    // Check if inserter committed
                    if (!HeapTupleHeaderXminCommitted(tuple.t_data)) {
                        all_visible = false;
                        *all_frozen = false;
                        break;
                    }

                    // Check if old enough for all to see as committed
                    xmin = HeapTupleHeaderGetXmin(tuple.t_data);
                    if (!TransactionIdPrecedes(xmin, vacrel->cutoffs.OldestXmin)) {
                        all_visible = false;
                        *all_frozen = false;
                        break;
                    }

                    // Track newest xmin on page
                    if (TransactionIdFollows(xmin, *visibility_cutoff_xid) &&
                        TransactionIdIsNormal(xmin))
                        *visibility_cutoff_xid = xmin;

                    // Check if tuple needs freezing
                    if (all_visible && *all_frozen &&
                        heap_tuple_needs_eventual_freeze(tuple.t_data))
                        *all_frozen = false;
                }
                break;

            case HEAPTUPLE_DEAD:
            case HEAPTUPLE_RECENTLY_DEAD:
            case HEAPTUPLE_INSERT_IN_PROGRESS:
            case HEAPTUPLE_DELETE_IN_PROGRESS:
                all_visible = false;
                *all_frozen = false;
                break;

            default:
                elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                break;
        }
    }

    // Clear offset info
    vacrel->offnum = InvalidOffsetNumber;

    return all_visible;
}
```