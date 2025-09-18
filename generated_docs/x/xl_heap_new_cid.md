# xl_heap_new_cid

## Location
src/include/access/heapam_xlog.h: 446 - 462

## Overview
A WAL record structure that logs new command ID (CID) assignments for tuple visibility tracking during transaction processing.

## Definition


## Detailed Description
The xl_heap_new_cid structure is used in PostgreSQL's Write-Ahead Logging system to record command ID assignments for tuple visibility. Command IDs are essential for PostgreSQL's MVCC system, allowing multiple commands within a single transaction to see consistent snapshots of data while maintaining isolation from other transactions.

This structure logs the assignment of command IDs (cmin/cmax) to specific tuples, which is crucial for logical replication and recovery. The structure stores both the transaction context and the specific tuple location to enable proper replay during recovery or logical decoding.

## Parameters / Member Variables
- : Top-level transaction ID to avoid merging CIDs from different transactions during recovery
- : Command ID when the tuple was created/inserted within the transaction
- : Command ID when the tuple was deleted/updated within the transaction  
- : Combined command ID used for debugging purposes
- : RelFileLocator identifying the specific relation file where the tuple resides
- : ItemPointerData (TID) specifying the exact tuple location within the relation

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type)
  - CommandId (type)
  - [RelFileLocator](../R/RelFileLocator.md) (type)
  - [ItemPointerData](../I/ItemPointerData.md) (type)
- Called from (representative examples):
  - [log_heap_new_cid](../l/log_heap_new_cid.md) (src/backend/access/heap/heapam.c:9040)
  - [heap2_desc](../h/heap2_desc.md) (src/backend/access/rmgrdesc/heapdesc.c:371)
  - [heap2_decode](../h/heap2_decode.md) (src/backend/replication/logical/decode.c:433,435)
  - [SnapBuildProcessNewCid](../S/SnapBuildProcessNewCid.md) (src/backend/replication/logical/snapbuild.c:829)
  - SizeOfHeapNewCid (src/include/access/heapam_xlog.h:464)

## Notes and Other Information
- Critical for logical replication as it enables proper command ID tracking across different nodes
- The top_xid field prevents confusion when merging CIDs from different transactions during recovery
- Used by the snapshot building mechanism in logical decoding to maintain transaction isolation
- The combocid field is primarily for debugging and troubleshooting visibility issues
- Essential for maintaining MVCC semantics during crash recovery and logical replication