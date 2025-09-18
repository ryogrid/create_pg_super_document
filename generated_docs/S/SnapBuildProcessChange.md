# SnapBuildProcessChange

## Location
src/backend/replication/logical/snapbuild.c: 778 - 827

## Overview
Determines whether changes made by a transaction at a specific LSN can be decoded, based on the current state of the snapshot builder.

## Definition
```c
bool SnapBuildProcessChange(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn)
```

## Detailed Description
This function is a critical component of PostgreSQL logical replication that manages when transaction changes can be safely decoded. It evaluates the current state of the snapshot builder and determines if changes from a specific transaction (identified by xid) at a given log position (lsn) are ready for decoding.

The function implements a state-based filtering mechanism:
1. First checks if the snapshot builder has reached a sufficient state (SNAPBUILD_FULL_SNAPSHOT)
2. For non-consistent states, filters out transactions that started before we had enough information
3. Ensures the reorder buffer has a base snapshot for the transaction, creating one if necessary

This is essential for maintaining consistency in logical replication by ensuring that only transactions with complete visibility information are processed.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the current state of snapshot building
- `xid`: Transaction ID of the change being processed
- `lsn`: Log Sequence Number indicating the position in the WAL where this change occurred

## Dependencies
- Functions called/Symbols referenced:
  - SNAPBUILD_FULL_SNAPSHOT (state constant)
  - SNAPBUILD_CONSISTENT (state constant)
  - TransactionIdPrecedes
  - ReorderBufferXidHasBaseSnapshot
  - SnapBuildBuildSnapshot
  - SnapBuildSnapIncRefcount
  - ReorderBufferSetBaseSnapshot
- Called from (representative examples):
  - heap2_decode (decode.c:426)
  - heap_decode (decode.c:490, 502, 508, 514, 538, 543)
  - logicalmsg_decode (decode.c:624)

## Notes and Other Information
- Returns false if the snapshot builder state is insufficient for processing changes
- Creates and manages snapshot reference counts to ensure proper memory management
- Critical for logical replication consistency by filtering out incomplete transaction information
- Part of the logical decoding infrastructure that enables features like logical replication slots