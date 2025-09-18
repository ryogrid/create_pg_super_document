# SnapBuildProcessNewCid

## Location
src/backend/replication/logical/snapbuild.c: 828 - 869

## Overview
Processes CommandId information from xl_heap_new_cid WAL records to track catalog modifications and maintain proper command sequencing for logical decoding.

## Definition
```c
void SnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn, xl_heap_new_cid *xlrec)
```

## Detailed Description
This function handles CommandId (CID) and combination CID processing when an xl_heap_new_cid record is encountered during WAL replay. These records are generated when transactions modify system catalog tuples, which is critical information for logical replication.

The function performs three main operations:
1. Marks the transaction as containing catalog changes, which affects visibility rules
2. Records tuple-specific CID information (cmin/cmax) for proper tuple visibility determination
3. Calculates and records the next command ID for the transaction

The CommandId tracking is essential for maintaining MVCC (Multi-Version Concurrency Control) semantics during logical decoding, ensuring that catalog changes are properly ordered and visible to the decoding process.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure managing snapshot state
- `xid`: Transaction ID that performed the catalog modification
- `lsn`: Log Sequence Number where the xl_heap_new_cid record is located
- `xlrec`: Pointer to the xl_heap_new_cid record containing CID information

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (data type)
  - [xl_heap_new_cid](../x/xl_heap_new_cid.md) (struct type)
  - [ReorderBufferXidSetCatalogChanges](../R/ReorderBufferXidSetCatalogChanges.md)
  - ReorderBufferAddNewTupleCids
  - InvalidCommandId (constant)
  - ReorderBufferAddNewCommandId
- Called from (representative examples):
  - [heap2_decode](../h/heap2_decode.md) (decode.c:436)

## Notes and Other Information
- Only processes records for transactions that have modified catalog tuples
- Handles both cmin and cmax values, taking the maximum when both are present
- Increments the command ID by 1 when recording the new command ID
- Part of the logical decoding infrastructure that ensures catalog changes are properly tracked
- Critical for maintaining transaction isolation and visibility rules during logical replication