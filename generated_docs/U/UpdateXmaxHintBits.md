# UpdateXmaxHintBits

## Location
[src/backend/access/heap/heapam.c:1949-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1949-L1970)

## Overview
This static function updates tuple hint bits in the heap after an xmax transaction completes, setting appropriate flags to indicate whether the transaction committed or aborted.

## Definition

```c
static void
UpdateXmaxHintBits(HeapTupleHeader tuple, Buffer buffer, TransactionId xid)
```
## Detailed Description
UpdateXmaxHintBits is called after waiting for an XMAX transaction to terminate to set appropriate hint bits that cache the transaction's final status. The function examines the transaction outcome and sets either HEAP_XMAX_COMMITTED or HEAP_XMAX_INVALID hint bits based on whether the transaction committed or aborted.

For lock-only transactions (those that only acquired tuple locks without modifying the tuple), the function sets HEAP_XMAX_INVALID even if the transaction committed, since lock-only transactions don't affect tuple visibility. The function ensures that callers can rely on checking only the XMAX_INVALID bit to determine if the xmax transaction is still relevant.

## Parameters / Member Variables
- `tuple`: Heap tuple header containing the xmax transaction ID and hint bits
- `buffer`: Buffer containing the tuple (may be marked dirty when hint bits are updated)
- `xid`: Transaction ID that should match the tuple's xmax value
## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawXmax: Extract the raw xmax transaction ID
  - TransactionIdEquals: Verify the xid matches the tuple's xmax
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md): Check if the transaction committed
  - [HeapTupleSetHintBits](../H/HeapTupleSetHintBits.md): Set the appropriate hint bits in the tuple
  - HEAP_XMAX_IS_LOCKED_ONLY: Macro to check if xmax was lock-only
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md): After waiting for concurrent delete operations
  - [heap_update](../h/heap_update.md): After waiting for concurrent update operations  
  - [heap_lock_tuple](../h/heap_lock_tuple.md): After waiting for concurrent lock operations

## Notes and Other Information
- This is a static function, only used within heapam.c
- Cannot be used for tuples whose xmax is a multixact (asserted)
- Sets HEAP_XMAX_INVALID for aborted transactions and lock-only committed transactions
- Sets HEAP_XMAX_COMMITTED for committed non-lock-only transactions
- Hint bits optimization: avoids future CLOG lookups by caching transaction status
- The function guarantees that XMAX_INVALID will be set for aborted transactions
- May not immediately set XMAX_COMMITTED for asynchronously committed transactions

## Simplified Source

```c
static void UpdateXmaxHintBits(HeapTupleHeader tuple, Buffer buffer, TransactionId xid) {
    // Validate xid matches tuple's xmax and it's not a multixact
    Assert(TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple), xid));
    Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

    // Only update if hint bits aren't already set
    if (!(tuple->t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID))) {
        if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) &&
            TransactionIdDidCommit(xid)) {
            // Non-lock-only transaction committed
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
        } else {
            // Transaction aborted OR it was lock-only
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
                               InvalidTransactionId);
        }
    }
}
```