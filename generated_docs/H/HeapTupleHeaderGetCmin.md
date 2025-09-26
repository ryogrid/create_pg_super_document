# HeapTupleHeaderGetCmin

## Location
[src/backend/utils/time/combocid.c:104-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L104-L117)

## Overview
Extracts the command ID (Cmin) from a heap tuple header, handling combo command IDs appropriately to return the actual minimum command ID within the current transaction.

## Definition
```c
CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tup)
```

## Detailed Description
This function retrieves the command ID that represents when a tuple was inserted (Cmin) within the current transaction. It handles the complexity of combo command IDs, which are used when a single transaction performs more than 62 commands. When a combo command ID is detected (via the HEAP_COMBOCID flag), it calls GetRealCmin() to resolve the actual minimum command ID from the combo command ID mapping.

The function includes assertions to ensure it's only called in valid contexts:
- The tuple must not have been moved by VACUUM FULL (HEAP_MOVED flag check)
- The tuple's Xmin must be the current transaction ID

## Parameters / Member Variables
- `tup`: Pointer to the heap tuple header from which to extract the Cmin value

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawCommandId: Gets the raw command ID from the tuple header
  - HeapTupleHeaderGetXmin: Gets the transaction ID that inserted the tuple
  - TransactionIdIsCurrentTransactionId: Validates that Xmin is the current transaction
  - GetRealCmin: Resolves combo command ID to actual minimum command ID
- Called from (representative examples):
  - log_heap_new_cid: For WAL logging purposes
  - heapam_tuple_lock: During tuple locking operations
  - HeapTupleSatisfiesUpdate: Visibility checking for updates
  - HeapTupleSatisfiesMVCC: MVCC visibility determination
  - HeapTupleHeaderAdjustCmax: Command ID adjustment operations

## Notes and Other Information
- Only valid to call from within the transaction that created the tuple
- Part of PostgreSQL's MVCC (Multi-Version Concurrency Control) system
- The combo command ID mechanism allows transactions to exceed the normal 62-command limit
- Located in src/backend/utils/time/combocid.c:104-117