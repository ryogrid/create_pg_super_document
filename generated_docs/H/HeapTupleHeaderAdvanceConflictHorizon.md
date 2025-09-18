# HeapTupleHeaderAdvanceConflictHorizon

## Location
src/backend/access/heap/heapam.c: 7950 - 7989

## Overview
Maintains a snapshot conflict horizon by advancing its value using committed transaction IDs from tuple headers, used during HOT pruning or index deletion operations to prevent recovery conflicts.

## Definition
```c
void HeapTupleHeaderAdvanceConflictHorizon(HeapTupleHeader tuple,
                                          TransactionId *snapshotConflictHorizon)
```

## Detailed Description
This function maintains the snapshotConflictHorizon for the caller by ratcheting forward its value using any committed XIDs contained in an obsolescent heap tuple that the caller is physically removing (e.g., via HOT pruning or index deletion). 

The function examines the tuple's transaction IDs (xmin, xmax, xvac) and updates the conflict horizon to ensure that recovery conflicts are properly handled during WAL replay. It specifically:

1. For HEAP_MOVED tuples, considers the xvac transaction ID
2. For committed tuples (determined by hint bits or clog lookup), considers the xmax transaction ID if it differs from xmin

The caller must initialize the snapshotConflictHorizon to InvalidTransactionId (interpreted as "no recovery conflict needed"). The final value must reflect all heap tuples that will be physically removed by the ongoing operation. During WAL replay, ResolveRecoveryConflictWithSnapshot() receives this final value from the caller's WAL record.

## Parameters / Member Variables
- `tuple`: HeapTupleHeader of the tuple being removed/pruned
- `snapshotConflictHorizon`: Input/output parameter tracking the latest committed XID that needs conflict resolution

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetUpdateXid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - HEAP_MOVED
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md)
  - [heap_prune_chain](../h/heap_prune_chain.md)

## Notes and Other Information
- Essential for maintaining consistency during hot standby operations where concurrent read-only queries may conflict with tuple removal
- The function only considers committed transactions to avoid spurious conflicts with aborted transactions
- Used primarily in HOT pruning and index tuple deletion scenarios
- The conflict horizon value is ultimately written to WAL records for proper replay handling
- Ignores tuples where the inserting transaction also updated/deleted the tuple (xmax == xmin)