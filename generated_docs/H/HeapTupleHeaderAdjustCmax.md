# HeapTupleHeaderAdjustCmax

## Location
[src/backend/utils/time/combocid.c:153-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L153-L181)

## Overview
Determines the correct command ID value to store in a tuple's t_cid field when the tuple is about to be deleted, creating a combo command ID if necessary.

## Definition
```c
void HeapTupleHeaderAdjustCmax(HeapTupleHeader tup, CommandId *cmax, bool *iscombo)
```

## Detailed Description
This function is responsible for determining whether a combo command ID is needed when marking a tuple for deletion. It handles a specific MVCC scenario: when a tuple that was inserted by the current transaction (or any subtransaction within it) is being deleted within the same transaction.

The function performs an optimization by checking HeapTupleHeaderXminCommitted() first (which is cheaper) before calling the more expensive TransactionIdIsCurrentTransactionId(). If both the tuple was inserted by the current transaction and is now being deleted, it creates a combo command ID that combines the original Cmin (insertion command) with the new Cmax (deletion command).

This operation is separated from HeapTupleHeaderSetCmax() because combo command ID creation can fail due to out-of-memory conditions, so it must be done before entering critical sections that modify shared buffers.

## Parameters / Member Variables
- `tup`: Pointer to the heap tuple header being processed
- `cmax`: Pointer to the command ID for the deletion operation; may be replaced with a combo CID
- `iscombo`: Pointer to boolean flag indicating whether a combo CID was created

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted: Checks if the tuple's Xmin is committed (optimization)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md): Verifies the insertion transaction is current
  - HeapTupleHeaderGetRawXmin: Gets the raw transaction ID that inserted the tuple
  - [HeapTupleHeaderGetCmin](HeapTupleHeaderGetCmin.md): Gets the insertion command ID
  - [GetComboCommandId](../G/GetComboCommandId.md): Creates a combo command ID from Cmin and Cmax
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md): Before deleting a tuple
  - [heap_update](../h/heap_update.md): Before updating a tuple (which involves deletion of old version)

## Notes and Other Information
- Essential for maintaining MVCC semantics when inserting and deleting within same transaction
- Must be called before entering critical sections due to potential memory allocation
- Optimization: checks committed status before expensive transaction ID comparison
- Part of PostgreSQL's combo command ID mechanism for transactions exceeding 62 commands
- The combo CID allows proper visibility determination for tuples modified multiple times in one transaction
- Located in src/backend/utils/time/combocid.c:153-181