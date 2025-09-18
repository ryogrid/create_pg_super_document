# heap_update

## Location
src/backend/access/heap/heapam.c: 3200 - 4181

## Overview
heap_update is the core function responsible for replacing a tuple in a heap table, handling complex visibility rules, hot updates, toast management, and multi-version concurrency control to ensure ACID compliance.

## Definition


## Detailed Description
heap_update performs the low-level replacement of a heap tuple with comprehensive transaction safety, concurrency control, and optimization strategies. This function is one of the most complex operations in PostgreSQL's heap access method, implementing sophisticated logic for:

**Hot Updates Optimization**: When possible, performs HOT (Heap-Only Tuple) updates that avoid index maintenance by placing the new tuple on the same page and not modifying indexed columns.

**Toast Management**: Handles out-of-line storage for large attributes, potentially compressing or moving data to separate TOAST tables.

**Concurrency Control**: Uses HeapTupleSatisfiesUpdate to check tuple visibility and manages complex multi-transaction scenarios, including waiting for conflicting operations and preserving necessary locks.

**Key Column Detection**: Analyzes which columns are being modified to determine appropriate locking levels - non-key updates can use weaker locks allowing more concurrency.

**Space Management**: Determines whether the updated tuple can fit on the same page or requires a new page, handling the complex buffer management and deadlock avoidance required for cross-page updates.

**Replica Identity**: Extracts and preserves replica identity information needed for logical replication.

The function operates through several phases:
1. **Preparation**: Validates parameters, determines column modifications, and acquires necessary bitmap sets
2. **Concurrency Handling**: Checks tuple visibility, handles concurrent modifications, and establishes appropriate locking
3. **Space Planning**: Determines if TOAST processing or new page allocation is needed
4. **Critical Section**: Updates tuple headers, manages visibility information, and logs changes
5. **Cleanup**: Handles resource cleanup and statistics updates

## Parameters / Member Variables
- : The heap relation containing the tuple to update
- : ItemPointer identifying the location of the tuple to be updated
- : HeapTuple containing the new tuple data to replace the old tuple
- : Command identifier for the current command within the transaction
- : Optional snapshot for additional visibility validation (used in RI checks)
- : Boolean indicating whether to wait for concurrent transactions or return immediately
- : Output structure containing failure details when update cannot proceed
- : Input/output parameter for the type of tuple lock required/acquired
- : Output parameter indicating which indexes need updating after the operation

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [HeapDetermineColumnsInfo](../H/HeapDetermineColumnsInfo.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)
  - [RelationGetBufferForTuple](../R/RelationGetBufferForTuple.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [log_heap_update](../l/log_heap_update.md)
  - [check_lock_if_inplace_updateable_rel](../c/check_lock_if_inplace_updateable_rel.md)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - [heap_freetuple](heap_freetuple.md)
- Called from (representative examples):
  - [simple_heap_update](../s/simple_heap_update.md)
  - [heapam_tuple_update](heapam_tuple_update.md)

## Notes and Other Information
- The function prohibits execution during parallel operations to prevent combo CID allocation issues
- Implements sophisticated HOT update optimization to avoid index maintenance when possible
- Handles both same-page and cross-page updates with appropriate deadlock prevention
- Supports summarized updates for improved performance with certain index types
- Uses critical sections to ensure atomic updates that can be properly recovered from crashes
- The function can return various TM_Result codes indicating success, conflicts, or conditions requiring caller attention
- Manages complex tuple locking scenarios including multi-transaction preservation
- Optimizes locking by using weaker locks when key columns are not modified
- Performs extensive validation including assertion checking in debug builds
- Handles toast table relationships correctly, never recursively toasting toast table entries