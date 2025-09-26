# TM_FailureData

## Location
src/include/access/tableam.h: 149 - 155

## Overview
TM_FailureData is a structure used to provide detailed failure information when table modification operations (update, delete, or lock) fail due to tuple concurrency conflicts in PostgreSQL's table access methods.

## Definition


## Detailed Description
This structure is filled in by table access methods when operations like table_tuple_update, table_tuple_delete, or table_tuple_lock fail because the target tuple is already outdated by another transaction. It provides the caller with essential information about what happened to the target tuple, enabling proper handling of concurrency conflicts and tuple versioning scenarios.

The structure captures the state of a tuple that has been modified or deleted by another transaction, providing both the location information and transaction details needed for the caller to make informed decisions about how to proceed.

## Parameters / Member Variables
- : The target tuple's ctid link - contains the same TID as the target if it was deleted, or points to the location of the replacement tuple if it was updated
- : The XID of the transaction that outdated the target tuple; set to InvalidTransactionId if the target was !LP_NORMAL (typically for TIDs from syscache)
- : The Command ID of the outdating command, but only valid when failure code is TM_SelfModified (current transaction modified the tuple); zero otherwise due to HeapTupleHeaderGetCmax limitations for cross-transaction scenarios  
- : Boolean flag indicating whether the tuple chain has been traversed during the operation

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type definition)
  - ItemPointerData (embedded structure)  
  - TransactionId (type definition)
- Called from (representative examples):
  - heap_delete
  - heap_update  
  - heap_lock_tuple
  - heapam_tuple_delete
  - heapam_tuple_update
  - heapam_tuple_lock
  - ExecOnConflictUpdate
  - RelationFindReplTupleByIndex

## Notes and Other Information
- This structure is central to PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation
- The cmax field has restricted validity to avoid issues with HeapTupleHeaderGetCmax when dealing with tuples modified by other transactions
- Used extensively in trigger execution, replication, and conflict resolution scenarios
- The structure enables proper tuple chain traversal and version tracking in concurrent environments