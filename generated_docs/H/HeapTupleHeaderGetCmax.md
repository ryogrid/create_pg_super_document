# HeapTupleHeaderGetCmax

## Location
[src/backend/utils/time/combocid.c:118-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L118-L152)

## Overview
Extracts the command ID (Cmax) from a heap tuple header, handling combo command IDs appropriately to return the actual maximum command ID within the current transaction.

## Definition
```c
CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tup)
```

## Detailed Description
This function retrieves the command ID that represents when a tuple was last modified (Cmax) within the current transaction. Similar to HeapTupleHeaderGetCmin, it handles combo command IDs by calling GetRealCmax() when the HEAP_COMBOCID flag is set. 

The function includes careful assertion logic that accounts for critical sections:
- The tuple must not have been moved by VACUUM FULL (HEAP_MOVED flag check)
- The tuple's update transaction ID (Xmax) should be the current transaction, but this check is weakened when inside a critical section to avoid memory allocations that GetUpdateXid() might perform for multixact values

This function is essential for MVCC visibility determinations and tuple modification operations.

## Parameters / Member Variables
- `tup`: Pointer to the heap tuple header from which to extract the Cmax value

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetRawCommandId: Gets the raw command ID from the tuple header
  - HeapTupleHeaderGetUpdateXid: Gets the transaction ID that last updated the tuple
  - TransactionIdIsCurrentTransactionId: Validates that the update transaction is current
  - GetRealCmax: Resolves combo command ID to actual maximum command ID
- Called from (representative examples):
  - heap_delete: During tuple deletion operations
  - heap_update: During tuple update operations  
  - heap_lock_tuple: During tuple locking operations
  - HeapTupleSatisfiesUpdate: Visibility checking for updates
  - HeapTupleSatisfiesMVCC: MVCC visibility determination
  - log_heap_new_cid: For WAL logging purposes

## Notes and Other Information
- Only valid to call from within the transaction that last modified the tuple
- Part of PostgreSQL's MVCC (Multi-Version Concurrency Control) system
- Contains special handling for critical sections to avoid memory allocations
- The combo command ID mechanism allows transactions to exceed the normal 62-command limit
- Cmax represents the command that deleted or updated the tuple
- Located in src/backend/utils/time/combocid.c:118-152