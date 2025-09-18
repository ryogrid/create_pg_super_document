# heap_delete

## Location
src/backend/access/heap/heapam.c: 2731 - 3153

## Overview
heap_delete is the core function responsible for deleting a tuple from a heap table in PostgreSQL, handling complex visibility rules, transaction concurrency, and logging to ensure ACID compliance.

## Definition


## Detailed Description
heap_delete performs the low-level deletion of a heap tuple with comprehensive transaction safety and concurrency control. The function follows PostgreSQL's multi-version concurrency control (MVCC) model, ensuring that concurrent transactions can safely access the same data without blocking each other inappropriately.

The function operates in several phases:
1. **Validation**: Checks for parallel operation restrictions and validates the tuple identifier
2. **Buffer Management**: Reads and locks the target page, managing visibility map interactions
3. **Concurrency Control**: Uses HeapTupleSatisfiesUpdate to check tuple visibility and handles concurrent modifications by waiting for conflicting transactions when necessary
4. **Conflict Resolution**: Manages multi-transaction scenarios and tuple locking to establish deletion priority
5. **Critical Section**: Updates tuple headers with deletion markers, manages visibility information, and logs the operation for crash recovery
6. **Cleanup**: Handles external TOAST data deletion, cache invalidation, and resource cleanup

The function is designed to handle edge cases like tuple updates during deletion attempts, serializable transaction conflicts, and partition moves. It maintains data consistency through careful transaction ID management and proper handling of tuple chains.

## Parameters / Member Variables
- : The heap relation containing the tuple to delete
- : ItemPointer identifying the specific tuple location (page and offset)
- : Command identifier for the current command within the transaction
- : Optional snapshot for additional visibility validation (used in RI checks)
- : Boolean indicating whether to wait for concurrent transactions or return immediately
- : Output structure containing failure details when deletion cannot proceed
- : Boolean flag indicating this deletion is part of a partition move operation

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [heap_toast_delete](heap_toast_delete.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [UpdateXmaxHintBits](../U/UpdateXmaxHintBits.md)
  - [xmax_infomask_changed](../x/xmax_infomask_changed.md)
  - [compute_infobits](../c/compute_infobits.md)
- Called from (representative examples):
  - [simple_heap_delete](../s/simple_heap_delete.md)
  - [heapam_tuple_delete](heapam_tuple_delete.md)

## Notes and Other Information
- The function prohibits execution during parallel operations to prevent combo CID allocation issues
- Implements sophisticated waiting mechanisms for concurrent transactions using tuple-level locking
- Handles both simple transaction waiting and complex multi-transaction conflicts
- Maintains replica identity information for logical replication purposes
- Supports partition movement operations through the changingPart parameter
- Performs extensive validation and assertion checking to ensure data consistency
- Uses critical sections to ensure atomic updates that can be properly recovered from crashes
- The function can return various TM_Result codes indicating success, conflicts, or other conditions requiring caller attention