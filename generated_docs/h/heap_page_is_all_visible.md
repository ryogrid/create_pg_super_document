# heap_page_is_all_visible

## Location
src/backend/access/heap/vacuumlazy.c: 2955 - 3070

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